"""Deterministic routing replay: integer log positions in a named lineage, never a live replica probe."""
import copy,hashlib,json
from .scan import SnapshotWindow,materialize,page_of,first_page_candidates

KV_KINDS=('kv_put','kv_delete')
SCAN_KIND='range_page'

def simulate(node_inputs,events,max_delay_ms,sticky_ms,snapshot_retention=8,scan_serving='pinned'):
    if not 1<=len(node_inputs)<=12:raise ValueError('需要1–12个节点')
    if not 1<=len(events)<=100:raise ValueError('需要1–100个时序事件')
    if max_delay_ms<0 or sticky_ms<0:raise ValueError('延迟门槛和粘滞时间不得为负')
    if not 1<=snapshot_retention<=50:raise ValueError('快照保留窗口须在1–50个版本之间')
    if scan_serving not in ('pinned','portable'):raise ValueError('快照策略仅支持 pinned 或 portable')
    if len({x['name'] for x in node_inputs})!=len(node_inputs):raise ValueError('节点名必须唯一')
    primaries=[x for x in node_inputs if x['role']=='primary']
    if len(primaries)!=1:raise ValueError('必须且只能配置一个主节点')
    nodes={x['name']:copy.deepcopy(x) for x in node_inputs};primary=nodes[primaries[0]['name']];lineage=primary['lineage']
    sessions={};scans={};mutations=[];trace=[];last_time=-1
    if any(n['role']=='replica' and n['lineage']==lineage and n['position']>primary['position'] for n in nodes.values()):raise ValueError('同日志谱系副本位点不得高于主库初始位点')
    # 每个节点的有限快照窗口：初始只保留“最近 retention 个版本”，更早快照视为已回收。
    windows={n['name']:SnapshotWindow(n['name'],max(0,n['position']-snapshot_retention+1),n['position']) for n in nodes.values()}
    mutation_positions=lambda:[p for p,_,_ in mutations]
    def advance_window(name,new_position,is_mutation):
        win=windows[name];win.advance(new_position)
        if is_mutation:win.note_version(new_position,snapshot_retention)
        else:win.sync_versions(mutation_positions(),snapshot_retention)
    def replica_checks(required,check_snapshot=None):
        rows=[]
        for n in nodes.values():
            if n['role']!='replica':continue
            win=windows[n['name']]
            row={'name':n['name'],'healthy':n['status']=='healthy','same_lineage':n['lineage']==lineage,
                 'position_sufficient':n['position']>=required,'delay_within_limit':n['delay_ms']<=max_delay_ms}
            if check_snapshot is not None:row['snapshot_retained']=win.serves(check_snapshot)
            rows.append(row)
        return rows
    def portable_candidates(snapshot_pos):
        eligible=[n['name'] for n in nodes.values() if n['role']=='replica' and n['status']=='healthy'
                  and n['lineage']==lineage and windows[n['name']].serves(snapshot_pos)
                  and n['delay_ms']<=max_delay_ms]
        eligible.sort(key=lambda nm:(nodes[nm]['delay_ms'],-nodes[nm]['position'],nm))
        return eligible
    for index,event in enumerate(events):
        at=event['at_ms'];kind=event['kind'];session=event.get('session','default')
        if at<last_time:raise ValueError('事件必须按时间非递减排列')
        last_time=at
        row={'index':index+1,'at_ms':at,'kind':kind,'session':session,'node':None,'decision':'applied','reason':'','required_position':sessions.get(session,{}).get('required',0),'observed_position':None,'safe':True}
        if kind in ('node_down','node_up','replicate'):
            target=nodes.get(event.get('node'))
            if not target:raise ValueError('事件引用未知节点：'+str(event.get('node')))
            row['node']=target['name']
            if kind=='replicate':
                if target['role']!='replica':raise ValueError('replicate仅允许副本节点')
                if target['status']!='healthy':raise ValueError('停机副本不能应用日志，请先node_up')
                if target['lineage']!=lineage:raise ValueError('日志谱系不匹配，不得复制当前主库日志')
                pos=event.get('position');delay=event.get('delay_ms')
                if pos is None or delay is None or delay<0 or not target['position']<=pos<=primary['position']:raise ValueError('复制位点必须单调且不超过已提交主库位点')
                target['position']=pos;target['delay_ms']=delay;advance_window(target['name'],pos,False)
                row['observed_position']=pos;row['reason']='副本应用已提交日志，快照窗口滑动至位点 %d，窗口下界 %d'%(pos,windows[target['name']].floor)
            else:
                target['status']='down' if kind=='node_down' else 'healthy';row['reason']='节点下线' if kind=='node_down' else '节点恢复'
        elif kind=='write':
            if primary['status']!='healthy':row.update(decision='rejected',reason='主库不可用，拒绝写入')
            else:
                primary['position']+=1;advance_window(primary['name'],primary['position'],False)
                sessions[session]={'required':primary['position'],'lineage':lineage,'last_write_ms':at}
                row.update(node=primary['name'],decision='routed',reason='写入主库并记录会话位点',required_position=primary['position'],observed_position=primary['position'])
        elif kind in KV_KINDS:
            key=event.get('key');value=event.get('value')
            if not isinstance(key,str) or not 1<=len(key)<=120:raise ValueError('KV事件需要1–120字符的key')
            if kind=='kv_put' and (not isinstance(value,str) or not 1<=len(value)<=500):raise ValueError('kv_put需要1–500字符的value')
            if primary['status']!='healthy':
                row.update(decision='rejected',reason='主库不可用，拒绝键值%s'%('写入' if kind=='kv_put' else '删除'))
            else:
                primary['position']+=1;pos=primary['position'];advance_window(primary['name'],pos,True)
                mutations.append((pos,key,value if kind=='kv_put' else None))
                sessions[session]={'required':pos,'lineage':lineage,'last_write_ms':at}
                row.update(node=primary['name'],decision='routed',observed_position=pos,required_position=pos,
                           reason=('键值写入主库，提交位点 %d'%pos) if kind=='kv_put' else ('键值删除（墓碑）提交位点 %d，快照内仍可重放'%pos))
                row['kv_key']=key
        elif kind=='read':
            state=sessions.get(session,{'required':0,'lineage':lineage,'last_write_ms':None});required=state['required'];row['required_position']=required
            sticky=state['last_write_ms'] is not None and at-state['last_write_ms']<sticky_ms
            eligible=[n for n in nodes.values() if n['role']=='replica' and n['status']=='healthy' and n['lineage']==state['lineage'] and n['position']>=required and n['delay_ms']<=max_delay_ms]
            candidates=sorted(eligible,key=lambda n:(n['delay_ms'],-n['position'],n['name']))
            selected=None
            if sticky:
                if primary['status']=='healthy' and primary['lineage']==state['lineage'] and primary['position']>=required:selected=primary;row['reason']='写后读粘滞：保持主库'
                else:row['reason']='粘滞期主库不可用，拒绝不确定读取'
            elif candidates:selected=candidates[0];row['reason']='副本位点满足会话且延迟达标'
            elif primary['status']=='healthy' and primary['lineage']==state['lineage'] and primary['position']>=required:selected=primary;row['reason']='无安全达标副本，回退主库'
            else:row['reason']='主库不可用且无安全达标副本，拒绝读取'
            if selected:row.update(node=selected['name'],decision='routed',observed_position=selected['position'],safe=selected['position']>=required and selected['lineage']==state['lineage'])
            else:row['decision']='rejected'
            row['replica_checks']=replica_checks(required)
        elif kind==SCAN_KIND:
            row.update(_scan_event(event,at,session,nodes,primary,lineage,sessions,scans,mutations,windows,
                                   snapshot_retention,scan_serving,max_delay_ms,replica_checks,portable_candidates))
        else:raise ValueError('未知事件类型')
        trace.append(row)
    requests=[r for r in trace if r['kind'] in ('read','write')+KV_KINDS+(SCAN_KIND,)]
    routed=[r for r in requests if r['decision']=='routed'];rejected=[r for r in requests if r['decision']=='rejected'];interrupted=[r for r in requests if r['decision']=='interrupted']
    safe=all(r['safe'] for r in routed)
    result={'mode':'受控复制路由模型 / 非真实复制集群','lineage':lineage,'trace':trace,'sessions':sessions,
            'scans':[{k:v for k,v in state.items() if k!='snapshot_items'} for state in scans.values()],
            'final_nodes':list(nodes.values()),
            'summary':{'requests':len(requests),'routed':len(routed),'rejected':len(rejected),'interrupted':len(interrupted),
                       'replica_reads':sum(r['kind']=='read' and r['node']!=primary['name'] for r in routed),
                       'primary_reads':sum(r['kind']=='read' and r['node']==primary['name'] for r in routed),
                       'scan_pages':sum(r['kind']==SCAN_KIND and r['decision']=='routed' for r in trace),
                       'all_routed_reads_safe':safe},
            'policy':{'max_delay_ms':max_delay_ms,'sticky_ms':sticky_ms,'position_model':'同一命名日志谱系的单调整数位点，不是完整GTID集合','sticky_boundary':'at_ms - last_write_ms < sticky_ms','replica_tie_break':'delay升序 / position降序 / name字典序'}}
    if any(r['kind'] in KV_KINDS+(SCAN_KIND,) for r in trace):
        result['snapshot_policy']={'snapshot_retention':snapshot_retention,'scan_serving':scan_serving,
            'snapshot_window':'每节点仅保留最近 %d 个日志版本，满窗淘汰最旧的未锚定快照'%snapshot_retention,
            'first_screen_rule':'首屏只选当前已持有所需快照的在线同谱系节点，不等待副本追平',
            'retry_rule':'retry=true 返回同一页且不推进游标',
            'pinned':'锚定首屏节点：快照受保护不被淘汰，但节点下线/快照丢失即中断，不可换节点',
            'portable':'不锚定：窗口内任一合格节点可续页、支持换节点；首屏多一次快照登记开销，快照被淘汰仍中断'}
    result['checksum']=hashlib.sha256(json.dumps(result,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
    return result

def _scan_event(event,at,session,nodes,primary,lineage,sessions,scans,mutations,windows,retention,scan_serving,max_delay_ms,replica_checks,portable_candidates):
    """处理单个 range_page 事件，返回 trace row 的业务字段。"""
    scan_id=event.get('scan_id');page_no=event.get('page_no');retry=bool(event.get('retry',False))
    if not isinstance(scan_id,str) or not 1<=len(scan_id)<=60:raise ValueError('range_page需要1–60字符的scan_id')
    if not isinstance(page_no,int) or page_no<1:raise ValueError('page_no必须为正整数')
    page_size=event.get('page_size')
    if page_size is not None and (not isinstance(page_size,int) or not 1<=page_size<=200):raise ValueError('page_size须在1–200之间')
    base={'scan_id':scan_id,'page_no':page_no,'retry':retry,'snapshot_position':None,'cursor_before':None,'cursor_after':None,'rows':[],'page_latency_ms':None}
    state=scans.get(scan_id)
    if page_no==1:
        if state is not None:raise ValueError('scan_id %s 已存在，首屏只能发起一次'%scan_id)
        start_key=event.get('start_key');end_key=event.get('end_key');page_size=page_size or 10
        if not isinstance(start_key,str) or not start_key:raise ValueError('首屏需要非空start_key')
        if end_key is not None and (not isinstance(end_key,str) or end_key<=start_key):raise ValueError('end_key必须严格大于start_key')
        required=sessions.get(session,{}).get('required',0)
        eligible,checks=first_page_candidates(windows,{k:v['role'] for k,v in nodes.items()},{k:v['status'] for k,v in nodes.items()},{k:v['lineage'] for k,v in nodes.items()},lineage,required,max_delay_ms,{k:v['delay_ms'] for k,v in nodes.items()})
        base['required_position']=required;base['replica_checks']=checks
        if not eligible:
            # 与普通 read 一致：无安全达标副本时回退主库；主库也不具备该快照才中断。
            pwin=windows[primary['name']]
            if primary['status']=='healthy' and primary['lineage']==lineage and primary['position']>=required and pwin.serves(required):
                eligible=[primary['name']];fallback_note='无安全达标副本，回退主库读取本地快照'
            else:
                held=[c['name'] for c in checks if c['snapshot_retained']]
                reason='首屏中断：没有在线同谱系副本持有会话位点 %d 的快照（不等待追平），且主库无法提供该快照。'%required
                if held:reason+='当前仅 %s 保留该版本，但不满足在线/谱系/延迟等其他资格条件'%('、'.join(held))
                else:reason+='所有节点的快照窗口均已回收该版本，或节点位点尚未到达该版本'
                base.update(node=None,decision='interrupted',reason=reason,observed_position=None,safe=False,scan_state='interrupted')
                scans[scan_id]={'scan_id':scan_id,'status':'interrupted','session':session,'snapshot_position':required,'start_key':start_key,'end_key':end_key,'page_size':page_size,'next_key':None,'anchor_node':None,'serving_nodes':[],'pages_served':0,'interrupted_reason':reason}
                return base
        else:fallback_note=None
        anchor=eligible[0];win=windows[anchor]
        items=materialize(mutations,required)
        rows,next_key=page_of(items,start_key,end_key,page_size)
        scan_state={'scan_id':scan_id,'status':'reading','session':session,'snapshot_position':required,'start_key':start_key,'end_key':end_key,'page_size':page_size,'next_key':next_key,'anchor_node':anchor,'serving_nodes':[anchor],'pages_served':1,'pinned':scan_serving=='pinned','snapshot_items':items}
        scans[scan_id]=scan_state
        if scan_serving=='pinned':
            win.pin(required);pin_note=('快照已在主库 %s 锚定，窗口淘汰受保护'%anchor) if anchor==primary['name'] else ('快照已在锚点 %s 锚定，窗口淘汰受保护'%anchor)
            latency=nodes[anchor]['delay_ms']
        else:
            pin_note='portable 策略不锚定节点，窗口内任一合格节点均可续页'+('（当前由主库兜底）' if anchor==primary['name'] else '')
            # 主库兜底无跨节点快照登记；副本首屏才有快照登记开销。
            latency=nodes[anchor]['delay_ms']+(20 if anchor!=primary['name'] else 0)
        finished=next_key is None
        if finished:
            scan_state['status']='finished'
            if scan_serving=='pinned':win.unpin(required)
        base.update(node=anchor,decision='routed',observed_position=required,snapshot_position=required,safe=True,
                    cursor_before=start_key,cursor_after=next_key,rows=rows,page_latency_ms=latency,scan_state=scan_state['status'],
                    reason='首屏按会话位点 %d 选中 %s 已保留的快照，%s%s；%s'%(required,anchor,pin_note,('（'+fallback_note+'）') if fallback_note else '','本页即为末页，扫描读完' if finished else '游标推进至 %s'%next_key))
        return base
    # 续页
    if state is None:raise ValueError('scan_id %s 尚未首屏，不能直接请求第 %d 页'%(scan_id,page_no))
    if state['status']!='reading':
        base.update(node=None,decision='rejected',observed_position=None,snapshot_position=state['snapshot_position'],
                    safe=False,scan_state=state['status'],reason='扫描已%s，拒绝新的分页请求（%s）'%('读完' if state['status']=='finished' else '中断',state.get('interrupted_reason','')))
        return base
    if page_no!=state['pages_served']+1 and not retry:
        raise ValueError('分页必须连续：scan_id %s 下一页应为第 %d 页'%(scan_id,state['pages_served']+1))
    if retry:
        # 幂等重试只针对“待执行的下一页”（如响应丢失后重发同一请求）：
        # 已推进的游标不会回退，因此不允许重发更早的页号；重试本身绝不推进游标。
        if state['pages_served']==0:raise ValueError('首屏尚未成功服务，没有可重试的页')
        if page_no!=state['pages_served']+1:raise ValueError('重试页号必须是待执行的第 %d 页，已读完的页不能用retry重发'%(state['pages_served']+1))
    snapshot_pos=state['snapshot_position']
    if scan_serving=='pinned':
        anchor=state['anchor_node'];anchor_node=nodes[anchor];win=windows[anchor]
        if anchor_node['status']!='healthy':
            reason='续页中断：pinned 锚点 %s 已下线，策略不允许换节点'%anchor
        elif anchor_node['lineage']!=lineage:
            reason='续页中断：pinned 锚点 %s 日志谱系已变化'%anchor
        elif not win.serves(snapshot_pos):
            reason='续页中断：锚点 %s 的快照窗口已回收位点 %d 的版本（窗口下界 %d）'%(anchor,snapshot_pos,win.floor)
        else:reason=''
        if reason:
            state['status']='interrupted';state['interrupted_reason']=reason;win.unpin(snapshot_pos)
            base.update(node=None,decision='interrupted',observed_position=None,snapshot_position=snapshot_pos,safe=False,scan_state='interrupted',
                        cursor_before=state['next_key'],reason=reason,replica_checks=replica_checks(snapshot_pos,snapshot_pos))
            return base
        serving=anchor
    else:
        eligible=portable_candidates(snapshot_pos)
        if not eligible:
            pwin=windows[primary['name']]
            if primary['status']=='healthy' and primary['lineage']==lineage and primary['position']>=snapshot_pos and pwin.serves(snapshot_pos):
                eligible=[primary['name']]
        if not eligible:
            reason='续页中断：没有任何在线同谱系节点保留快照位点 %d（窗口均已回收或节点不可用，含主库兜底），无法换节点续读'%snapshot_pos
            state['status']='interrupted';state['interrupted_reason']=reason
            base.update(node=None,decision='interrupted',observed_position=None,snapshot_position=snapshot_pos,safe=False,scan_state='interrupted',
                        cursor_before=state['next_key'],reason=reason,replica_checks=replica_checks(snapshot_pos,snapshot_pos))
            return base
        serving=eligible[0]
        if serving not in state['serving_nodes']:state['serving_nodes'].append(serving)
    cursor_before=state['next_key']
    if retry:
        # 重试：用当前游标重新取同一页，绝不推进游标与页计数。
        if cursor_before is None:raise ValueError('扫描已读完，没有可重试的页')
        rows,_=page_of(state['snapshot_items'],cursor_before,state['end_key'],state['page_size'])
        base.update(node=serving,decision='routed',observed_position=snapshot_pos,snapshot_position=snapshot_pos,safe=True,
                    cursor_before=cursor_before,cursor_after=cursor_before,rows=rows,page_latency_ms=nodes[serving]['delay_ms'],scan_state='reading',
                    reason='重试第 %d 页：游标保持 %s 不推进，仍读快照位点 %d 的同一页数据'%(state['pages_served'],cursor_before,snapshot_pos))
        return base
    rows,next_key=page_of(state['snapshot_items'],cursor_before,state['end_key'],state['page_size'])
    state['next_key']=next_key;state['pages_served']+=1
    finished=next_key is None
    if finished:
        state['status']='finished'
        if scan_serving=='pinned':windows[state['anchor_node']].unpin(snapshot_pos)
    base.update(node=serving,decision='routed',observed_position=snapshot_pos,snapshot_position=snapshot_pos,safe=True,
                cursor_before=cursor_before,cursor_after=next_key,rows=rows,page_latency_ms=nodes[serving]['delay_ms'],scan_state=state['status'],
                reason=('续页由 %s 提供快照位点 %d，游标 %s → %s'%(serving,snapshot_pos,cursor_before,next_key if next_key is not None else '末页'))+('，扫描读完' if finished else ''))
    return base

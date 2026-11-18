"""范围分页扫描引擎的确定性验证（纯标准库，不依赖 MySQL/Redis/FastAPI）。"""
import sys,copy
sys.path.insert(0,'/workspace/backend')
from src.services.routing import simulate

def nodes(primary_pos=100,north=(95,150),south=(100,20)):
    return [
        {'name':'harbor-primary','role':'primary','status':'healthy','lineage':'qiyuan-log-01','position':primary_pos,'delay_ms':0},
        {'name':'north-replica','role':'replica','status':'healthy','lineage':'qiyuan-log-01','position':north[0],'delay_ms':north[1]},
        {'name':'south-replica','role':'replica','status':'healthy','lineage':'qiyuan-log-01','position':south[0],'delay_ms':south[1]},
    ]

def trace_map(result):return {r['index']:r for r in result['trace']}
def scan_rows(result):return [r for r in result['trace'] if r['kind']=='range_page']
def check(name,cond,detail=''):
    if not cond:raise AssertionError('FAIL %s %s'%(name,detail))
    print('ok -',name,detail)

# 1) 翻页期间插删：读到的始终是首屏快照，且按 key 顺序、无丢失无重复
def test_pagination_sees_snapshot_not_live_writes():
    ev=[
        {'at_ms':0,'kind':'kv_put','session':'lab-A','key':'k1','value':'v1'},       #101
        {'at_ms':10,'kind':'kv_put','session':'lab-A','key':'k2','value':'v2'},      #102
        {'at_ms':20,'kind':'kv_put','session':'lab-A','key':'k3','value':'v3'},      #103
        {'at_ms':30,'kind':'replicate','node':'south-replica','position':103,'delay_ms':20},
        {'at_ms':40,'kind':'range_page','session':'lab-A','scan_id':'s1','page_no':1,'start_key':'k1','page_size':2},
        # 翻页期间并发写入与删除
        {'at_ms':50,'kind':'kv_put','session':'lab-A','key':'k2','value':'v2-new'},
        {'at_ms':60,'kind':'kv_delete','session':'lab-A','key':'k3'},
        {'at_ms':70,'kind':'kv_put','session':'lab-A','key':'k4','value':'v4'},
        {'at_ms':80,'kind':'replicate','node':'south-replica','position':106,'delay_ms':20},
        {'at_ms':90,'kind':'range_page','session':'lab-A','scan_id':'s1','page_no':2},
    ]
    r=simulate(nodes(),ev,100,500,snapshot_retention=8,scan_serving='pinned')
    pages=scan_rows(r)
    check('两页都成功',all(p['decision']=='routed' for p in pages),str([(p['page_no'],p['decision']) for p in pages]))
    p1,p2=pages
    check('首屏含k1,k2旧值',[x['key'] for x in p1['rows']]==['k1','k2'] and p1['rows'][1]['value']=='v2',str(p1['rows']))
    check('续页仍是快照里的k3（删除不可见）',[x['key'] for x in p2['rows']]==['k3'] and p2['rows'][0]['value']=='v3',str(p2['rows']))
    check('游标推进 k1->k3->末页',p1['cursor_after']=='k3' and p2['cursor_after'] is None)
    check('扫描状态读完',r['scans'][0]['status']=='finished')
    check('全程锚定south',p1['node']=='south-replica' and p2['node']=='south-replica')

# 2) 重试不推进游标，返回同一页
def test_retry_keeps_cursor():
    ev=[
        {'at_ms':0,'kind':'kv_put','session':'lab-A','key':'a','value':'1'},
        {'at_ms':5,'kind':'kv_put','session':'lab-A','key':'b','value':'2'},
        {'at_ms':10,'kind':'kv_put','session':'lab-A','key':'c','value':'3'},
        {'at_ms':20,'kind':'replicate','node':'south-replica','position':103,'delay_ms':20},
        {'at_ms':30,'kind':'range_page','session':'lab-A','scan_id':'s2','page_no':1,'start_key':'a','page_size':1},
        {'at_ms':40,'kind':'range_page','session':'lab-A','scan_id':'s2','page_no':2,'retry':True},
        {'at_ms':50,'kind':'range_page','session':'lab-A','scan_id':'s2','page_no':2},
        {'at_ms':60,'kind':'range_page','session':'lab-A','scan_id':'s2','page_no':3},
    ]
    r=simulate(nodes(),ev,100,500)
    pages=scan_rows(r)
    check('全部成功',all(p['decision']=='routed' for p in pages))
    retry=pages[1]
    check('重试游标不推进',retry['cursor_before']=='b' and retry['cursor_after']=='b' and [x['key'] for x in retry['rows']]==['b'])
    check('重试后页号2仍取到b，页号3取c',[x['key'] for x in pages[2]['rows']]==['b'] and [x['key'] for x in pages[3]['rows']]==['c'])
    check('总服务页数为3（重试不计新页）',r['scans'][0]['pages_served']==3)

# 2b) 已服务过的页不能用 retry 重发（游标不回退），非法页号在引擎层 422
def test_retry_served_page_rejected():
    ev=[
        {'at_ms':0,'kind':'kv_put','session':'lab-A','key':'a','value':'1'},
        {'at_ms':5,'kind':'kv_put','session':'lab-A','key':'b','value':'2'},
        {'at_ms':10,'kind':'kv_put','session':'lab-A','key':'c','value':'3'},
        {'at_ms':20,'kind':'replicate','node':'south-replica','position':103,'delay_ms':20},
        {'at_ms':30,'kind':'range_page','session':'lab-A','scan_id':'s2b','page_no':1,'start_key':'a','page_size':1},
        {'at_ms':40,'kind':'range_page','session':'lab-A','scan_id':'s2b','page_no':2},
        {'at_ms':50,'kind':'range_page','session':'lab-A','scan_id':'s2b','page_no':2,'retry':True},
    ]
    try:
        simulate(nodes(),ev,100,500);raise AssertionError('应拒绝重发已读页')
    except ValueError as e:check('重发已读页被拒绝','已读完的页不能用retry重发' in str(e),str(e))

# 3) portable 可换节点；锚点下线后续页成功
def test_portable_failover():
    ev=[
        {'at_ms':0,'kind':'kv_put','session':'lab-A','key':'a','value':'1'},
        {'at_ms':10,'kind':'kv_put','session':'lab-A','key':'b','value':'2'},
        {'at_ms':20,'kind':'replicate','node':'south-replica','position':102,'delay_ms':20},
        {'at_ms':25,'kind':'replicate','node':'north-replica','position':102,'delay_ms':50},
        {'at_ms':30,'kind':'range_page','session':'lab-A','scan_id':'s3','page_no':1,'start_key':'a','page_size':1},
        {'at_ms':40,'kind':'node_down','node':'south-replica'},
        {'at_ms':50,'kind':'range_page','session':'lab-A','scan_id':'s3','page_no':2},
    ]
    r=simulate(nodes(),ev,100,500,snapshot_retention=8,scan_serving='portable')
    pages=scan_rows(r)
    check('首屏选延迟最低的south',pages[0]['node']=='south-replica')
    check('south下线后portable换到north',pages[1]['decision']=='routed' and pages[1]['node']=='north-replica',pages[1].get('reason'))
    check('服务节点列表含两个',set(r['scans'][0]['serving_nodes'])=={'south-replica','north-replica'})

# 4) pinned 锚点下线 => 中断，不可换节点
def test_pinned_anchor_down_interrupts():
    ev=[
        {'at_ms':0,'kind':'kv_put','session':'lab-A','key':'a','value':'1'},
        {'at_ms':5,'kind':'kv_put','session':'lab-A','key':'b','value':'2'},
        {'at_ms':10,'kind':'replicate','node':'south-replica','position':102,'delay_ms':20},
        {'at_ms':15,'kind':'replicate','node':'north-replica','position':102,'delay_ms':50},
        {'at_ms':20,'kind':'range_page','session':'lab-A','scan_id':'s4','page_no':1,'start_key':'a','page_size':1},
        {'at_ms':30,'kind':'node_down','node':'south-replica'},
        {'at_ms':40,'kind':'range_page','session':'lab-A','scan_id':'s4','page_no':2},
        {'at_ms':50,'kind':'range_page','session':'lab-A','scan_id':'s4','page_no':3},
    ]
    r=simulate(nodes(),ev,100,500,scan_serving='pinned')
    pages=scan_rows(r)
    check('续页中断',pages[1]['decision']=='interrupted' and 'pinned' in pages[1]['reason'])
    check('中断后再来页被拒绝',pages[2]['decision']=='rejected')
    check('扫描状态interrupted',r['scans'][0]['status']=='interrupted')

# 5) 首屏不等待追平：副本没有快照且主库也不可用 => 中断并说明
def test_first_screen_no_wait_interrupts():
    ev=[
        {'at_ms':0,'kind':'kv_put','session':'lab-A','key':'a','value':'1'},  #101 仅主库
        {'at_ms':5,'kind':'node_down','node':'harbor-primary'},
        {'at_ms':10,'kind':'range_page','session':'lab-A','scan_id':'s5','page_no':1,'start_key':'a'},
    ]
    r=simulate(nodes(),ev,100,500)
    p=scan_rows(r)[0]
    check('首屏中断不等待',p['decision']=='interrupted' and '不等待追平' in p['reason'],p['reason'])

# 5b) 副本未追平时首屏回退主库，不等待副本
def test_first_screen_primary_fallback():
    ev=[
        {'at_ms':0,'kind':'kv_put','session':'lab-A','key':'a','value':'1'},  #101 仅主库
        {'at_ms':10,'kind':'range_page','session':'lab-A','scan_id':'s5b','page_no':1,'start_key':'a'},
    ]
    r=simulate(nodes(),ev,100,500,scan_serving='portable')
    p=scan_rows(r)[0]
    check('首屏回退主库',p['decision']=='routed' and p['node']=='harbor-primary' and p['rows'][0]=={'key':'a','value':'1'},p.get('reason'))
    check('主库兜底无登记开销',p['page_latency_ms']==0)

# 6) 快照窗口有限：版本被淘汰后续页中断（portable），并展示窗口下界
def test_snapshot_eviction_interrupts():
    ev=[{'at_ms':0,'kind':'kv_put','session':'lab-A','key':'a','value':'1'},
        {'at_ms':5,'kind':'kv_put','session':'lab-A','key':'b','value':'2'}]  #101-102
    ev.append({'at_ms':10,'kind':'replicate','node':'south-replica','position':102,'delay_ms':20})
    ev.append({'at_ms':20,'kind':'range_page','session':'lab-A','scan_id':'s6','page_no':1,'start_key':'a','page_size':1})
    # 再写入并让副本推进，retention=2 时旧快照102被淘汰
    for i in range(3):
        ev.append({'at_ms':30+i*10,'kind':'kv_put','session':'lab-A','key':'k%d'%i,'value':'x'})
    ev.append({'at_ms':70,'kind':'replicate','node':'south-replica','position':105,'delay_ms':20})
    ev.append({'at_ms':80,'kind':'range_page','session':'lab-A','scan_id':'s6','page_no':2})
    r=simulate(nodes(),ev,100,500,snapshot_retention=2,scan_serving='portable')
    pages=scan_rows(r)
    check('淘汰导致中断',pages[1]['decision']=='interrupted' and '回收' in pages[1]['reason'],pages[1]['reason'])

# 7) pinned 锚定保护快照不被淘汰，续页仍成功
def test_pinned_protects_from_eviction():
    ev=[{'at_ms':0,'kind':'kv_put','session':'lab-A','key':'a','value':'1'},
        {'at_ms':5,'kind':'kv_put','session':'lab-A','key':'b','value':'2'}]
    ev.append({'at_ms':10,'kind':'replicate','node':'south-replica','position':102,'delay_ms':20})
    ev.append({'at_ms':20,'kind':'range_page','session':'lab-A','scan_id':'s7','page_no':1,'start_key':'a','page_size':1})
    for i in range(3):
        ev.append({'at_ms':30+i*10,'kind':'kv_put','session':'lab-A','key':'k%d'%i,'value':'x'})
    ev.append({'at_ms':70,'kind':'replicate','node':'south-replica','position':105,'delay_ms':20})
    ev.append({'at_ms':80,'kind':'range_page','session':'lab-A','scan_id':'s7','page_no':2})
    r=simulate(nodes(),ev,100,500,snapshot_retention=2,scan_serving='pinned')
    pages=scan_rows(r)
    check('锚定保护，续页成功',pages[1]['decision']=='routed',pages[1].get('reason'))

# 8) portable 首屏延迟含登记开销，且比 pinned 慢
def test_first_screen_latency_tradeoff():
    ev=[
        {'at_ms':0,'kind':'kv_put','session':'lab-A','key':'a','value':'1'},
        {'at_ms':10,'kind':'replicate','node':'south-replica','position':101,'delay_ms':20},
        {'at_ms':20,'kind':'range_page','session':'lab-A','scan_id':'s8','page_no':1,'start_key':'a'},
    ]
    pinned=simulate(nodes(),ev,100,500,scan_serving='pinned')
    portable=simulate(nodes(),ev,100,500,scan_serving='portable')
    check('pinned首屏20ms',scan_rows(pinned)[0]['page_latency_ms']==20)
    check('portable首屏40ms（20+登记20）',scan_rows(portable)[0]['page_latency_ms']==40)

# 9) 旧请求集（无KV事件）结果结构与 checksum 行为不变
def test_legacy_events_unchanged():
    ev=[{'at_ms':0,'kind':'write','session':'lab-A'},{'at_ms':100,'kind':'read','session':'lab-A'}]
    r=simulate(nodes(105,(104,150),(105,20)),ev,100,500)
    check('旧结果无snapshot_policy','snapshot_policy' not in r)
    check('旧汇总字段保留',all(k in r['summary'] for k in ('requests','routed','rejected','replica_reads','primary_reads','all_routed_reads_safe')))
    import json,hashlib
    payload={k:v for k,v in r.items() if k!='checksum'}
    expect=hashlib.sha256(json.dumps(payload,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
    check('checksum可复算',r['checksum']==expect)

# 10) 非法输入在引擎层被拒绝
def test_validation():
    base=[{'at_ms':0,'kind':'range_page','session':'x','scan_id':'z','page_no':2,'start_key':'a'}]
    for bad,msg in [
        ([{'at_ms':0,'kind':'range_page','session':'x','scan_id':'z','page_no':2}],'续页未首屏'),
        ([{'at_ms':0,'kind':'kv_put','session':'x','key':'k'}],'kv_put缺value'),
        ([{'at_ms':0,'kind':'range_page','session':'x','scan_id':'z','page_no':1}],'首屏缺start_key'),
    ]:
        try:
            simulate(nodes(),bad,100,500);raise AssertionError('应拒绝: '+msg)
        except ValueError:print('ok - 拒绝非法输入:',msg)

if __name__=='__main__':
    test_pagination_sees_snapshot_not_live_writes()
    test_retry_keeps_cursor()
    test_retry_served_page_rejected()
    test_portable_failover()
    test_pinned_anchor_down_interrupts()
    test_first_screen_no_wait_interrupts()
    test_first_screen_primary_fallback()
    test_snapshot_eviction_interrupts()
    test_pinned_protects_from_eviction()
    test_first_screen_latency_tradeoff()
    test_legacy_events_unchanged()
    test_validation()
    print('\n全部扫描引擎测试通过 ✔')

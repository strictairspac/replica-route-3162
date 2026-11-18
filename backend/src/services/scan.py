"""范围分页扫描引擎：在有限快照窗口上提供可重复读的键序分页。

设计要点（与受控复制路由模型一致，全部为确定性计算，不连接真实集群）：
- 全局键空间按字典序维护，kv_put/kv_delete 在主库提交，产生单调整数位点的变更日志；
  delete 以墓碑标记保留，保证快照可重放，翻页期间的增删不影响已选快照。
- 每个节点只保留一个“有限快照窗口”[floor, position]：窗口外的旧快照无法服务。
  快照空间按“数据变更版本”计数，只有键值写/删产生新的物化快照；纯日志复制不产生
  新版本，不会因此淘汰旧快照。窗口满时淘汰最旧的未锚定版本，模拟快照空间有限。
- 首屏（page_no=1）按会话所需位点选择“当前已经持有该快照”的在线同谱系节点，
  绝不等待副本追平；没有任何节点持有所需快照时，首屏中断并说明原因。
- pinned 策略锚定首屏节点：锚定期间该快照受保护不被淘汰，但节点下线即中断、不可换节点。
- portable 策略不锚定：任何仍保留该快照版本的合格节点都可续页，可换节点；
  代价是首屏多一次快照登记开销，且窗口滑动后快照被淘汰仍会中断。
- 重试（retry=true）返回同一页且不推进游标；扫描 finished 后再请求按拒绝处理。
"""
from bisect import bisect_left


class SnapshotWindow:
    """节点上有限的快照保留窗口。

    floor：仍可服务的最旧快照版本（含）。position：节点当前日志位点。
    versions：本节点已知的数据变更位点（有序），窗口按版本数而非位点步长淘汰。
    pinned：被 pinned 扫描锚定、禁止淘汰的快照位点集合。
    """

    __slots__ = ('name', 'floor', 'position', 'pinned', 'versions')

    def __init__(self, name, floor, position):
        self.name = name
        self.floor = floor
        self.position = position
        self.pinned = set()
        self.versions = []

    def advance(self, new_position):
        """位点前进：窗口上界抬升；floor 由淘汰策略单独维护。"""
        if new_position < self.position:
            raise ValueError('节点位点不能回退')
        self.position = new_position

    def note_version(self, mutation_pos, retention):
        """登记一个数据变更版本，并按保留窗口淘汰最旧的未锚定快照。"""
        if not self.versions or self.versions[-1] < mutation_pos:
            self.versions.append(mutation_pos)
        self._recompute_floor(retention)

    def sync_versions(self, mutation_positions, retention):
        """复制推进时，把主库截至本节点位点的变更版本并入窗口。"""
        for pos in mutation_positions:
            if pos <= self.position and pos not in self.versions:
                self.versions.append(pos)
        self.versions.sort()
        self._recompute_floor(retention)

    def _recompute_floor(self, retention):
        retained = self.versions[-retention:]
        floor = retained[0] if retained else self.floor
        if self.pinned:
            floor = min(floor, min(self.pinned))
        if floor > self.floor:
            self.floor = floor

    def serves(self, snapshot_pos):
        return self.floor <= snapshot_pos <= self.position

    def pin(self, snapshot_pos):
        self.pinned.add(snapshot_pos)

    def unpin(self, snapshot_pos):
        self.pinned.discard(snapshot_pos)


def materialize(mutations, snapshot_pos):
    """重放截至 snapshot_pos 的变更日志，返回该快照下的有序存活键值列表。

    mutations: [(position, key, value|None)]，value 为 None 表示删除墓碑。
    """
    live = {}
    for pos, key, value in mutations:
        if pos > snapshot_pos:
            break
        if value is None:
            live.pop(key, None)
        else:
            live[key] = value
    return sorted(live.items())


def page_of(snapshot_items, start_key, end_key, page_size):
    """从有序快照中切出 [start_key, end_key) 范围内的一页。

    start_key 为游标下界（含），返回 (本页行, 下一页游标键或 None)。
    """
    keys = [k for k, _ in snapshot_items]
    lo = bisect_left(keys, start_key)
    rows = []
    next_index = lo
    for idx in range(lo, len(snapshot_items)):
        key, value = snapshot_items[idx]
        if end_key is not None and key >= end_key:
            next_index = idx
            break
        if len(rows) < page_size:
            rows.append({'key': key, 'value': value})
            next_index = idx + 1
        else:
            next_index = idx
            break
    next_key = None
    if next_index < len(snapshot_items):
        nxt_key = snapshot_items[next_index][0]
        if end_key is None or nxt_key < end_key:
            next_key = nxt_key
    return rows, next_key


def first_page_candidates(windows, node_roles, node_status, lineages, lineage, required_pos, max_delay_ms, delays):
    """首屏选节点：在线、同谱系、已持有所需快照且延迟达标的副本，按既定次序排序。"""
    eligible = []
    checks = []
    for name, win in windows.items():
        if node_roles[name] != 'replica':
            continue
        healthy = node_status[name] == 'healthy'
        same_lineage = lineages[name] == lineage
        position_sufficient = win.position >= required_pos
        snapshot_retained = win.serves(required_pos)
        delay_within = delays[name] <= max_delay_ms
        checks.append({'name': name, 'healthy': healthy, 'same_lineage': same_lineage,
                       'position_sufficient': position_sufficient,
                       'snapshot_retained': snapshot_retained, 'delay_within_limit': delay_within})
        if healthy and same_lineage and position_sufficient and snapshot_retained and delay_within:
            eligible.append(name)
    eligible.sort(key=lambda n: (delays[n], -windows[n].position, n))
    return eligible, checks

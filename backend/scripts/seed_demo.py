from sqlalchemy import select,func
from src.config.database import Session,Base,engine
from src.config.database import ensure_columns
from src.models.entities import User,Node,RequestSet,Trial
from src.utils.security import password_hash

def seed():
    Base.metadata.create_all(engine)
    ensure_columns()
    with Session() as db:
        for role in ('admin','operator','viewer'):
            email=role+'@qiyuan.test'
            if not db.scalar(select(User).where(User.email==email)):db.add(User(email=email,password=password_hash('Demo2026!'),role=role))
        for name,role,pos,delay,note in [('harbor-primary','primary',105,0,'栖原主站 · 写入航道'),('north-replica','replica',104,150,'北站副本 · 延迟偏高'),('south-replica','replica',105,20,'南站副本 · 常用读航道')]:
            if not db.scalar(select(Node).where(Node.name==name)):db.add(Node(name=name,role=role,status='healthy',lineage='qiyuan-log-01',position=pos,delay_ms=delay,note=note))
        mixed=[{'at_ms':0,'kind':'write','session':'lab-A'},{'at_ms':100,'kind':'read','session':'lab-A'},{'at_ms':600,'kind':'replicate','node':'south-replica','position':106,'delay_ms':20},{'at_ms':700,'kind':'read','session':'lab-A'},{'at_ms':800,'kind':'node_down','node':'south-replica'},{'at_ms':900,'kind':'read','session':'lab-A'},{'at_ms':1000,'kind':'node_down','node':'harbor-primary'},{'at_ms':1100,'kind':'read','session':'lab-A'}]
        # 范围分页读：首屏锚定快照，翻页期间并发插删不影响快照；重试不推进游标；末页读完
        paged=[
            {'at_ms':0,'kind':'kv_put','session':'lab-KV','key':'key-01','value':'栖原·航道A'},
            {'at_ms':5,'kind':'kv_put','session':'lab-KV','key':'key-02','value':'栖原·航道B'},
            {'at_ms':10,'kind':'kv_put','session':'lab-KV','key':'key-03','value':'栖原·航道C'},
            {'at_ms':15,'kind':'kv_put','session':'lab-KV','key':'key-04','value':'栖原·航道D'},
            {'at_ms':20,'kind':'kv_put','session':'lab-KV','key':'key-05','value':'栖原·航道E'},
            {'at_ms':30,'kind':'replicate','node':'south-replica','position':110,'delay_ms':20},
            {'at_ms':40,'kind':'range_page','session':'lab-KV','scan_id':'scan-d1','page_no':1,'start_key':'key-01','page_size':2},
            {'at_ms':50,'kind':'kv_put','session':'lab-KV','key':'key-02','value':'栖原·航道B-新版'},
            {'at_ms':60,'kind':'kv_delete','session':'lab-KV','key':'key-03'},
            {'at_ms':70,'kind':'kv_put','session':'lab-KV','key':'key-06','value':'栖原·航道F'},
            {'at_ms':80,'kind':'replicate','node':'south-replica','position':113,'delay_ms':20},
            {'at_ms':90,'kind':'range_page','session':'lab-KV','scan_id':'scan-d1','page_no':2},
            {'at_ms':95,'kind':'range_page','session':'lab-KV','scan_id':'scan-d1','page_no':3,'retry':True},
            {'at_ms':100,'kind':'range_page','session':'lab-KV','scan_id':'scan-d1','page_no':3},
        ]
        # portable 换节点 + 快照淘汰：南站下线后续页改由北站；窄窗口下版本回收后中断
        failover=[
            {'at_ms':0,'kind':'kv_put','session':'lab-FO','key':'alpha','value':'α'},
            {'at_ms':10,'kind':'kv_put','session':'lab-FO','key':'beta','value':'β'},
            {'at_ms':20,'kind':'replicate','node':'south-replica','position':107,'delay_ms':20},
            {'at_ms':25,'kind':'replicate','node':'north-replica','position':107,'delay_ms':50},
            {'at_ms':30,'kind':'range_page','session':'lab-FO','scan_id':'scan-d2','page_no':1,'start_key':'a','page_size':1},
            {'at_ms':40,'kind':'node_down','node':'south-replica'},
            {'at_ms':50,'kind':'range_page','session':'lab-FO','scan_id':'scan-d2','page_no':2},
        ]
        cases=[('QY-1117-A','交接班写后读与双节点故障',mixed),('QY-1117-B','延迟超标回退主库',[{'at_ms':0,'kind':'node_down','node':'south-replica'},{'at_ms':10,'kind':'read','session':'visitor'}]),('QY-1117-C','双副本不可读时拒绝',[{'at_ms':0,'kind':'write','session':'lab-B'},{'at_ms':1000,'kind':'node_down','node':'harbor-primary'},{'at_ms':1100,'kind':'read','session':'lab-B'}]),('QY-1117-D','范围分页读：快照隔离与重试',paged),('QY-1117-E','范围分页读：换节点续页',failover)]
        for code,title,events in cases:
            if not db.scalar(select(RequestSet).where(RequestSet.code==code)):db.add(RequestSet(code=code,title=title,events=events,note='栖原站点值班交接 · 09:15 请求轨迹'))
        db.commit();print({cls.__tablename__:db.scalar(select(func.count()).select_from(cls)) for cls in (User,Node,RequestSet,Trial)})
if __name__=='__main__':seed()

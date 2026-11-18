/** Node:{id,name,role,status,lineage,position,delay_ms}; RequestSet:{id,code,title,events,note}; Trial:{id,name,request_set_id,max_delay_ms,sticky_ms,snapshot_retention,scan_serving,input_snapshot,status,result,error}
 *  range_page 事件：{at_ms,kind:'range_page',session,scan_id,page_no,start_key?,end_key?,page_size?,retry?}
 *  扫描结果 result.scans[]：{scan_id,status(reading/finished/interrupted),snapshot_position,anchor_node,serving_nodes,pages_served,interrupted_reason?} */
export const entityNames=['nodes','request_sets','trials'];

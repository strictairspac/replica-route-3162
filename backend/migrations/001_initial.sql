-- Initial schema is applied idempotently by SQLAlchemy metadata.create_all.
-- Authoritative entities: users, nodes, request_sets, trials, audit_logs.
-- Routing request events and frozen input/result evidence are MySQL JSON.

-- 范围分页读特性：演练新增快照窗口与续页策略两个参数。
-- 对已存在的库，应用启动时 src.config.database.ensure_columns 会幂等执行同等补列；
-- 全新库由 metadata.create_all 直接带上默认值建表，此文件仅作离线 DDL 留档。
ALTER TABLE trials
    ADD COLUMN snapshot_retention INT NOT NULL DEFAULT 8,
    ADD COLUMN scan_serving VARCHAR(20) NOT NULL DEFAULT 'pinned';

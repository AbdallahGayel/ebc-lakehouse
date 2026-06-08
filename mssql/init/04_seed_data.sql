-- =============================================================================
-- mssql/init/04_seed_data.sql
-- Seed data — same 20 representative rows as the old Cassandra keyspace,
-- translated to T-SQL syntax. Guarded with NOT EXISTS so re-runs are no-ops.
-- =============================================================================
USE ebc_atm;
GO

IF NOT EXISTS (SELECT 1 FROM dbo.atm_sessions)
BEGIN
INSERT INTO dbo.atm_sessions
    (atm_id, session_date, session_ts, card_token, issuing_bank_id, txn_type,
     amount_egp, currency_code, response_code, [status], atm_location,
     governorate, network_id, processing_ms, error_code, created_at, updated_at)
VALUES
    ('ATM001001','2026-04-17','2026-04-17 08:12:31','tk_88102','CIB','WITHDRAWAL',     500,'EGP','00','APPROVED','Tahrir Square','Cairo','123',1240,NULL,'2026-04-17 08:12:31','2026-04-17 08:12:31'),
    ('ATM001002','2026-04-17','2026-04-17 08:45:00','tk_88103','NBE','BALANCE_INQUIRY',  0,'EGP','00','APPROVED','Mohandessin','Giza','123',820,NULL,'2026-04-17 08:45:00','2026-04-17 08:45:00'),
    ('ATM002001','2026-04-17','2026-04-17 09:00:15','tk_88104','QNB','WITHDRAWAL',    1000,'EGP','51','DECLINED','Smouha','Alexandria','123',980,'INSUFFICIENT_FUNDS','2026-04-17 09:00:15','2026-04-17 09:00:15'),
    ('ATM002002','2026-04-17','2026-04-17 09:30:44','tk_88105','BDC','WITHDRAWAL',     200,'EGP','00','APPROVED','Mansheyya','Alexandria','SHARED',1100,NULL,'2026-04-17 09:30:44','2026-04-17 09:30:44'),
    ('ATM003001','2026-04-17','2026-04-17 10:00:00','tk_88106','MIB','MINI_STATEMENT',   0,'EGP','00','APPROVED','City Centre','Mansoura','123',760,NULL,'2026-04-17 10:00:00','2026-04-17 10:00:00'),
    ('ATM003002','2026-04-17','2026-04-17 10:30:22','tk_88107','HSBC','WITHDRAWAL',   3000,'EGP','00','APPROVED','Corniche','Aswan','123',1350,NULL,'2026-04-17 10:30:22','2026-04-17 10:30:22'),
    ('ATM004001','2026-04-17','2026-04-17 11:00:05','tk_88108','FAB','TRANSFER',       500,'EGP','00','APPROVED','Mall of Arabia','Giza','123',2100,NULL,'2026-04-17 11:00:05','2026-04-17 11:00:05'),
    ('ATM004002','2026-04-17','2026-04-17 11:45:30','tk_88109','AGB','WITHDRAWAL',     800,'EGP','14','DECLINED','Port Said Port','Port Said','SHARED',850,'INVALID_CARD','2026-04-17 11:45:30','2026-04-17 11:45:30'),
    ('ATM005001','2026-04-17','2026-04-17 12:00:00','tk_88110','CIB','WITHDRAWAL',     400,'EGP','00','APPROVED','Suez Canal','Suez','123',1050,NULL,'2026-04-17 12:00:00','2026-04-17 12:00:00'),
    ('ATM005002','2026-04-17','2026-04-17 12:30:15','tk_88111','NBE','BALANCE_INQUIRY',  0,'EGP','00','APPROVED','New Capital','Cairo','123',640,NULL,'2026-04-17 12:30:15','2026-04-17 12:30:15'),
    ('ATM001001','2026-04-18','2026-04-18 07:00:01','tk_88112','QNB','WITHDRAWAL',    1500,'EGP','00','APPROVED','Tahrir Square','Cairo','123',1180,NULL,'2026-04-18 07:00:01','2026-04-18 07:00:01'),
    ('ATM001002','2026-04-18','2026-04-18 07:30:44','tk_88113','BDC','WITHDRAWAL',     200,'EGP','00','APPROVED','Mohandessin','Giza','123',990,NULL,'2026-04-18 07:30:44','2026-04-18 07:30:44'),
    ('ATM002001','2026-04-18','2026-04-18 08:15:22','tk_88114','MIB','WITHDRAWAL',     600,'EGP','00','APPROVED','Smouha','Alexandria','123',1300,NULL,'2026-04-18 08:15:22','2026-04-18 08:15:22'),
    ('ATM003001','2026-04-18','2026-04-18 09:00:00','tk_88115','HSBC','MINI_STATEMENT',  0,'EGP','00','APPROVED','City Centre','Mansoura','123',720,NULL,'2026-04-18 09:00:00','2026-04-18 09:00:00'),
    ('ATM004001','2026-04-18','2026-04-18 09:30:10','tk_88116','FAB','WITHDRAWAL',    1000,'EGP','61','DECLINED','Mall of Arabia','Giza','123',870,'EXCEEDS_LIMIT','2026-04-18 09:30:10','2026-04-18 09:30:10'),
    ('ATM005001','2026-04-18','2026-04-18 10:00:33','tk_88117','AGB','WITHDRAWAL',     300,'EGP','00','APPROVED','Suez Canal','Suez','SHARED',1020,NULL,'2026-04-18 10:00:33','2026-04-18 10:00:33'),
    ('ATM001001','2026-04-18','2026-04-18 10:45:00','tk_88118','CIB','WITHDRAWAL',     700,'EGP','00','APPROVED','Tahrir Square','Cairo','123',1150,NULL,'2026-04-18 10:45:00','2026-04-18 10:45:00'),
    ('ATM002002','2026-04-18','2026-04-18 11:00:55','tk_88119','NBE','BALANCE_INQUIRY',  0,'EGP','00','APPROVED','Mansheyya','Alexandria','123',680,NULL,'2026-04-18 11:00:55','2026-04-18 11:00:55'),
    ('ATM003002','2026-04-18','2026-04-18 11:30:20','tk_88120','QNB','WITHDRAWAL',    2000,'EGP','00','APPROVED','Corniche','Aswan','123',1400,NULL,'2026-04-18 11:30:20','2026-04-18 11:30:20'),
    ('ATM004002','2026-04-18','2026-04-18 12:00:00','tk_88121','BDC','WITHDRAWAL',     400,'EGP','00','APPROVED','Port Said Port','Port Said','SHARED',950,NULL,'2026-04-18 12:00:00','2026-04-18 12:00:00');
END
GO

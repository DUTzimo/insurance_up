# ---------------------------------------------------------------------

 /export/server/sqoop/bin/sqoop import --connect jdbc:mysql://192.168.88.166:3306/insurance --username root --password 123456 --table claim_info --hive-table insurance_ods.claim_info --target-dir /user/hive/warehouse/insurance_ods.db/claim_info --delete-target-dir --fields-terminated-by '\t'  -m 1;

# ---------------------------------------------------------------------

 /export/server/sqoop/bin/sqoop import --connect jdbc:mysql://192.168.88.166:3306/insurance --username root --password 123456 --table policy_benefit --hive-table insurance_ods.policy_benefit --target-dir /user/hive/warehouse/insurance_ods.db/policy_benefit --delete-target-dir --fields-terminated-by '\t'  -m 1;

# ---------------------------------------------------------------------

 /export/server/sqoop/bin/sqoop import --connect jdbc:mysql://192.168.88.166:3306/insurance --username root --password 123456 --table policy_client --hive-table insurance_ods.policy_client --target-dir /user/hive/warehouse/insurance_ods.db/policy_client --delete-target-dir --fields-terminated-by '\t'  -m 1;

# ---------------------------------------------------------------------

 /export/server/sqoop/bin/sqoop import --connect jdbc:mysql://192.168.88.166:3306/insurance --username root --password 123456 --table policy_surrender --hive-table insurance_ods.policy_surrender --target-dir /user/hive/warehouse/insurance_ods.db/policy_surrender --delete-target-dir --fields-terminated-by '\t'  -m 1;


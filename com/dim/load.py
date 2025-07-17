from pyspark.sql import SparkSession
def get_spark_session():
    spark = SparkSession.builder \
        .appName("HiveDimETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark
def load_complex(spark):
    try:
        sql="""with cx as (select id,
                   complex_name,
                   province_id,
                   city_id,
                   district_id,
                   district_name
            from ods_base_complex
            where ds = '20250717'
              and is_deleted = 0),
     pv as (select id,
                   name
            from ods_base_region_info
            where is_deleted = 0),
     cy as (select id,
                   name
            from ods_base_region_info
            where is_deleted = 0),
     ex as (select collect_set(cast(courier_emp_id as string)) courier_emp_id,
                   complex_id
            from ods_express_courier_complex
            where ds = '20250717'
              and is_deleted = 0
            group by complex_id)
insert overwrite table dim_complex_full partition(dt = '2025-07-17')
select cx.id,
       complex_name,
       courier_emp_id,
       province_id,
       pv.name,
       city_id,
       cy.name,
       district_id,
       district_name
from cx
         left join pv
                   on cx.province_id = pv.id
         left join cy
                   on cx.city_id = pv.id
         left join ex
                   on cx.id = ex.complex_id"""
        spark.sql(sql)
        print("数据导入成功")

    except Exception as e:
        print(f"数据导入失败: {str(e)}")
        raise
def load_organ(spark):
    try:
        sql="""with og as (select id,
                   org_name,
                   org_level,
                   region_id,
                   org_parent_id
            from ods_base_organ
            where ds = '20250717'
              and is_deleted = 0),
     rg as (select id,
                   name,
                   dict_code
            from ods_base_region_info where is_deleted = 0)
insert overwrite table dim_organ_full partition(dt = '2025-07-17')
select a.id,
       a.org_name,
       a.org_level,
       a.region_id,
       rg.name,
       dict_code,
       a.org_parent_id,
       pog.org_name
from og a
         left join rg
                   on a.region_id = rg.id
         left join og pog
                   on a.org_parent_id = pog.id"""
        spark.sql(sql)
        print("数据导入成功")

    except Exception as e:
        print(f"数据导入失败: {str(e)}")
        raise
def load_region(spark):
    try:
        sql="""insert overwrite table dim_region_full partition (dt = '2025-07-17')
select id,
       parent_id,
       name,
       dict_code,
       short_name
from ods_base_region_info where is_deleted = 0"""
        spark.sql(sql)
        print("数据导入成功")

    except Exception as e:
        print(f"数据导入失败: {str(e)}")
        raise
def load_express_courier(spark):
    try:
        sql="""with ex as (
select id,
       emp_id,
       org_id,
       working_phone,
       express_type
    from ods_express_courier where is_deleted=0
),
rg as (
    select
        id,
        org_name
    from ods_base_organ where ds='20250717' and is_deleted=0
),dc as (
    select
        id,name
    from ods_base_dic where is_deleted=0
)
insert overwrite table dim_express_courier_full partition (dt='2025-07-17')
select
    ex.id,
emp_id,
org_id,
rg.org_name,
working_phone,
express_type,
dc.name
from ex left join rg
on ex.org_id=rg.id
left join dc
on ex.express_type=dc.id"""
        spark.sql(sql)
        print("数据导入成功")

    except Exception as e:
        print(f"数据导入失败: {str(e)}")
        raise
def load_line_base_shift(spark):
    try:
        sql="""with sf as (
    select
        id,
        line_id,
        start_time,
        driver1_emp_id,
        driver2_emp_id,
        truck_id,
        pair_shift_id
    from ods_line_base_shift where ds='20250717' and is_deleted=0
), le as (
    select
        id,
        name,
        line_no,
        line_level,
        org_id,
        transport_line_type_id,
        start_org_id,
        start_org_name,
        end_org_id,
        end_org_name,
        pair_line_id,
        distance,
        cost,
        estimated_time,
        status
    from ods_line_base_info where ds='20250717' and is_deleted=0
), bc as (
    select
        id,name
    from ods_base_dic where is_deleted=0
)
insert overwrite table dim_shift_full partition (dt='20250717')
select
sf.id,
line_id,
le.name,
line_no,
line_level,
org_id,
transport_line_type_id,
bc.name,
start_org_id,
start_org_name,
end_org_id,
end_org_name,
pair_line_id,
distance,
cost,
estimated_time,
start_time,
driver1_emp_id,
driver2_emp_id,
truck_id,
pair_shift_id
from sf left join le
on sf.line_id = le.id
left join bc
on le.transport_line_type_id=bc.id"""
        spark.sql(sql)
        print("数据导入成功")

    except Exception as e:
        print(f"数据导入失败: {str(e)}")
        raise
if __name__ == '__main__':
    spark = get_spark_session()
    load_complex(spark)
    load_organ(spark)
    load_region(spark)
    load_express_courier(spark)
    load_line_base_shift(spark)

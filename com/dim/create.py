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

def get_dim_complex(spark):
    try:
        drop_sql = "drop table if exists dim_complex_full"
        sql = """
create external table dim_complex_full(
    id bigint comment '小区ID',
    complex_name string comment '小区名称',
    courier_emp_ids array<string> comment '负责快递员IDS',
    province_id string comment '省份ID',
    province_name string comment '省份名称',
    city_id string comment '城市ID',
    city_name string comment '城市名称',
    district_id bigint comment '区 / 县ID',
    district_name string comment '区 / 县名称'
)comment '小区区维度表'
partitioned by (dt string)
stored as orc
location '/bigdata_warehouse/tms/dim/dim_complex_full'
tblproperties ('orc.compress'='snappy')
        """
        spark.sql(drop_sql)
        spark.sql(sql)
        print("表 dim_shift_full 创建成功")
    except Exception as e:
        print(f"创建表时出错: {str(e)}")
        raise


def get_dim_organ(spark):
    try:
        drop_sql = "drop table if exists dim_organ_full"
        sql = """
create external table dim_organ_full(
    id string comment '机构ID',
    org_name string comment '机构名称',
    org_level string comment '机构等级(1为转运中心，2转运站)',
    region_id string comment '地区ID，1级机构为city，2级机构为district',
    region_name string comment '地区名称',
    region_code string comment '地区编码（行政级别）',
    org_parent_id string comment '父级机构ID',
    org_parent_mame string comment '父级机构名称'
)comment '机构维度表'
partitioned by (dt string)
stored as orc
location '/bigdata_warehouse/tms/dim/dim_organ_full'
tblproperties ('orc.compress'='snappy')
        """
        spark.sql(drop_sql)
        spark.sql(sql)
        print("表 dim_shift_full 创建成功")
    except Exception as e:
        print(f"创建表时出错: {str(e)}")
        raise

def get_dim_region(spark):
    try:
        drop_sql = "drop table if exists dim_region_full"
        sql = """
create external table dim_region_full
(
    id         bigint COMMENT '地区ID',
    parent_id  bigint COMMENT '上级地区ID',
    name       string COMMENT '地区名称',
    dict_code  string COMMENT '编码（行政级别）',
    short_name string COMMENT "简称"
)comment '地区维度表'
partitioned by (dt string)
stored as orc
location '/bigdata_warehouse/tms/dim/dim_region_full'
tblproperties('orc.compress'='snappy')
        """
        spark.sql(drop_sql)
        spark.sql(sql)
        print("表 dim_shift_full 创建成功")
    except Exception as e:
        print(f"创建表时出错: {str(e)}")
        raise

def get_dim_express_courier(spark):
    try:
        drop_sql = "drop table if exists dim_express_courier_full"
        sql = """
create external table dim_express_courier_full(
id bigint COMMENT'快递员ID',
emp_id bigint COMMENT '员工ID',
org_id bigint COMMENT'所属机构ID',
org_name string COMMENT'机构名称',
working_phone string COMMENT'工作电话',
express_type string COMMENT'快递员类型(收货;发货)',
express_type_name string COMMENT '快递员类型名称'
)comment'快递员维度表'
partitioned by (dt string comment'统计日期')
stored as orc
location '/bigdata_warehouse/tms/dim/dim_express_courier_full'
tblproperties ('orc.compress'='snappy')
        """
        spark.sql(drop_sql)
        spark.sql(sql)
        print("表 dim_shift_full 创建成功")
    except Exception as e:
        print(f"创建表时出错: {str(e)}")
        raise
def get_dim_shift(spark):
    try:
        drop_sql = "drop table if exists dim_shift_full"
        sql = """
create external table dim_shift_full(
id bigint COMMENT'班次ID',
line_id bigint COMMENT'线路ID',
line_name string COMMENT'线路名称',
line_no string COMMENT'线路编号',
line_level string COMMENT'线路级别',
org_id bigint COMMENT'所属机构',
transport_line_type_id string COMMENT'线路类型ID',
transport_line_type_name string COMMENT'线路类型名称',
start_org_id bigint COMMENT'起始机构ID',
start_org_name string COMMENT'起始机构名称',
end_org_id bigint COMMENT'目标机构ID',
end_org_name string COMMENT'目标机构名称',
pair_line_id bigint COMMENT'配对线路ID',
distance decimal(10,2) COMMENT'直线距离',
`cost` decimal(10,2) COMMENT'公路里程',
estimated_time bigint COMMENT'预计时间(分钟)',
start_time string COMMENT'班次开始时间',
driver1_emp_id bigint COMMENT'第一司机',
driver2_emp_id bigint COMMENT'第二司机',
truck_id bigint COMMENT'卡车ID',
pair_shift_id bigint COMMENT'配对班次(同一辆车一去一回的另一班次)'
)comment '班次维度表'
partitioned by (dt string comment 'iRitMM')
stored as orc
Location '/bigdata_warehouse/tms/dim/dim_shift_full'
tblproperties('orc.compress'='snappy')
        """
        spark.sql(drop_sql)
        spark.sql(sql)
        print("表 dim_shift_full 创建成功")
    except Exception as e:
        print(f"创建表时出错: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        spark = get_spark_session()
        get_dim_complex(spark)
        get_dim_organ(spark)
        get_dim_region(spark)
        get_dim_express_courier(spark)
        get_dim_shift(spark)
    except Exception as e:
        print(f"执行过程中发生错误: {str(e)}")
        sys.exit(1)
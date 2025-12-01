import sqlite3
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import atexit
import logging

# 配置日志，用于捕获服务器端的异常信息
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
# 确保 CORS 配置正确
CORS(app, resources={r"/api/*": {"origins": "*", "allow_headers": ["Content-Type", "X-Auth-Key"], "methods": ["GET", "POST", "OPTIONS"]}})

# ======================================================
# 关键配置
# ======================================================
API_PASSWORD = os.getenv('API_PASSWORD', 'OldXiangHuang_600')
CSV_FILE = 'traindata.csv'
DB_FILE = 'train_data.db'
TABLE_NAME = 'traindata'
AUTH_KEY = 'temp_auth_token_for_demo'
# ======================================================

# 全局数据库连接对象
db_conn = None

# 列名映射（保持不变，注意：新字段“查询用车组号”不需要在这里，因为它仅用于后台查询）
COLUMN_MAP = [
    "总序号", "车组号", "车内布局", "运用状态", "运用属性", "配属局", "配属段", "配属所",
    "车型", "批次", "制造厂", "制造日期", "最高运营速度（km/h）", "设计寿命（年）", "列车总长（m）",
    "车体最大宽度（mm）", "车体最大高度（mm）", "车体最大高度（mm）", "轮周牵引总功率（kW）", "停放制动能力", "编组方式",
    "双弓间距（m）", "受电弓位置（车厢）", "编组（辆）", "EMIS定员", "席位总数", "客票定员",
    "商务座-包间式", "商务座-鱼骨式", "商务座-标准型", "特等座", "优选一等座", "一等座", "二等座",
    "高级动卧", "动卧", "多功能座", "餐座", "无座", "非对号定员-餐座", "非对号定员-边座",
    "非对号定员-其他", "非对号定员-无座",
    "01车车种", "01车定员", "02车车种", "02车定员", "03车车种", "03车定员",
    "04车车种", "04车定员", "05车车种", "05车定员", "06车车种", "06车定员",
    "07车车种", "07车定员", "08车车种", "08车定员", "09车车种", "09车定员",
    "10车车种", "10车定员", "11车车种", "11车定员", "12车车种", "12车定员",
    "13车车种", "13车定员", "14车车种", "14车定员", "15车车种", "15车定员",
    "16车车种", "16车定员", "17车车种", "17车定员",
    "市域车所属线路", "特殊涂装", "备注", "座椅类型"
]
# 筛选器键到 SQL 列的映射（保持不变）
FILTER_COL_MAP = {
    'attr': '运用属性', 'bureau': '配属局', 'depot': '配属段', 'location': '配属所',
    'model': '车型', 'factory': '制造厂', 'car_count': '编组（辆）'
}

def get_db_connection():
  """返回一个新的 SQLite 连接。"""
  return sqlite3.connect(DB_FILE, check_same_thread=False)

def init_sqlite_db():
  """读取 CSV 文件，创建并填充 SQLite 数据库。"""
  global db_conn
  if os.path.exists(DB_FILE):
    logging.info(f"数据库文件 {DB_FILE} 已存在，跳过初始化。")
    db_conn = get_db_connection()
    return
  logging.info(f"正在从 {CSV_FILE} 初始化数据库...")
  try:
    # 假设 CSV 中已包含 "查询用车组号" 字段
    df = pd.read_csv(CSV_FILE, encoding='utf-8', skipinitialspace=True, engine='python')
    df.columns = df.columns.str.strip()
    with get_db_connection() as conn:
      df.to_sql(TABLE_NAME, conn, if_exists='replace', index=False)
      conn.commit()
    logging.info(f"成功创建表 '{TABLE_NAME}'，行数: {len(df)}")
    db_conn = get_db_connection()
  except FileNotFoundError:
    logging.error(f"错误: 数据文件 {CSV_FILE} 未找到! 数据库无法初始化。")
  except Exception as e:
    logging.error(f"数据库初始化错误: {e}")

@atexit.register
def close_db_connection():
  if db_conn:
    db_conn.close()
    logging.info("数据库连接已关闭。")

with app.app_context():
  init_sqlite_db()


@app.route('/api/login', methods=['POST'])
def login():
  """用户登录验证 (模拟)"""
  data = request.get_json()
  password = data.get('password')
  if password == API_PASSWORD:
    return jsonify({'message': 'Login successful', 'auth_key': AUTH_KEY}), 200
  else:
    return jsonify({'message': 'Invalid credentials'}), 401


@app.route('/api/traindata', methods=['POST'])
def get_filter_config():
  """获取筛选器配置。"""
  auth_key = request.headers.get('X-Auth-Key')
  if auth_key != AUTH_KEY or not db_conn:
    return jsonify({'message': 'Authorization required or database not ready'}), 401
  try:
    filter_columns = ['配属局', '配属段', '运用属性', '车型', '制造厂', '编组（辆）']
    filters = {}
    for col in filter_columns:
      distinct_df = pd.read_sql_query(f"SELECT DISTINCT \"{col}\" FROM \"{TABLE_NAME}\" WHERE \"{col}\" IS NOT NULL", db_conn)
      values = distinct_df[col].tolist()
      if col == '编组（辆）':
        # 确保编组（辆）值是字符串，与前端 Select2 兼容
        filters['car_counts'] = [str(x) for x in values]
      elif col == '配属局':
        filters['bureaus'] = values
      elif col == '配属段':
        filters['depots'] = values
      elif col == '运用属性':
        filters['attrs'] = values
      elif col == '车型':
        filters['models'] = values
      elif col == '制造厂':
        filters['factories'] = values
    cols_to_fetch = ['"配属局"', '"配属段"', '"配属所"']
    map_data_df = pd.read_sql_query(f"SELECT {', '.join(cols_to_fetch)} FROM \"{TABLE_NAME}\"", db_conn)
    map_data_list = map_data_df.to_dict('records')
    return jsonify({'map_data': map_data_list, 'filters': filters})
  except Exception as e:
    logging.error(f"API 获取筛选配置错误: {e}")
    return jsonify({'message': '服务器遇到数据库错误。'}), 500


def build_where_clause(custom_filters, search_value):
  """
  根据自定义筛选器和 DataTables 搜索值构建 SQL WHERE 子句和参数列表。
  DataTables 的全局搜索 (search_value) 和自定义 'train' 搜索均针对新字段 "查询用车组号"。
  """
  where_clauses = []
  params = []
 
  # 1. 应用多选筛选器 (保持不变)
  for key, col in FILTER_COL_MAP.items():
    values = custom_filters.get(key)
    clean_values = [v for v in values if v is not None and str(v).strip() != ''] if isinstance(values, list) else []
    
    if clean_values:
      placeholders = ', '.join(['?' for _ in clean_values])
      where_clauses.append(f"\"{col}\" IN ({placeholders})")
      params.extend(clean_values)
   
  # 2. 应用自定义 'train' 筛选 (**使用新字段**)
  train_search = custom_filters.get('train')
  if train_search and str(train_search).strip() != '':
    # 用户输入去连字符，与新字段匹配
    normalized_train_search = train_search.replace('-', '').strip()
    train_search_pattern = f'%{normalized_train_search}%'
    
    # 匹配新字段 "查询用车组号"
    where_clauses.append(f"(\"查询用车组号\" LIKE ?)")
    params.append(train_search_pattern)


  # 3. 应用 DataTables 全局搜索 (**仅针对新字段**)
  if search_value and str(search_value).strip() != '':
    # 标准化搜索值：仅移除所有连字符（-）
    normalized_search_value = search_value.replace('-', '').strip()
    search_pattern = f'%{normalized_search_value}%'
    
    # 匹配新字段 "查询用车组号"
    where_clauses.append(f"(\"查询用车组号\" LIKE ?)")
    params.append(search_pattern)
            
  where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
  return where_sql, params


@app.route('/api/serverside/traindata', methods=['POST'])
@app.route('/api/data', methods=['POST'])
def serverside_traindata():
  """DataTables Server-side Processing API。"""
  auth_key = request.headers.get('X-Auth-Key')
  if auth_key != AUTH_KEY or not db_conn:
    return jsonify({'message': 'Authorization required or database not ready'}), 401

  try:
    # 1. 强化请求体解析
    data = request.get_json()
    if data is None:
        logging.error("request.get_json() returned None. Check Content-Type header on the client side.")
        return jsonify({'message': '请求数据无效，请检查Content-Type或JSON格式。'}), 400 
        
    draw = int(data.get('draw', 0))
    start = int(data.get('start', 0))
    length = int(data.get('length', 25))

    # DataTables 全局搜索值
    search_value = data.get('search', {}).get('value', '')
    
    # 修复 IndexError
    order_list = data.get('order', [])
    order_data = order_list[0] if order_list else {}
    
    order_column_index = order_data.get('column')
    order_dir = order_data.get('dir', 'asc')

    custom_filters = data.get('custom_filters', {})
   
    # 2. 构建 WHERE 子句和参数
    where_sql, params = build_where_clause(custom_filters, search_value)
   
    # DEBUG: 打印正在执行的筛选条件，这是诊断问题的关键
    logging.info(f"Filters - Custom: {custom_filters}, Global Search: '{search_value}'")
    logging.info(f"SQL Components - WHERE: '{where_sql}', Params: {params}")
    
    # 3. 统计总数
    cursor = db_conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM \"{TABLE_NAME}\"")
    records_total = cursor.fetchone()[0]

    # 4. 统计筛选后的总数
    count_query = f"SELECT COUNT(*) FROM \"{TABLE_NAME}\"{where_sql}"
    cursor.execute(count_query, params)
    records_filtered = cursor.fetchone()[0]
    
    # 5. 确定排序
    order_sql = ""
    if order_column_index is not None and order_column_index != '' and int(order_column_index) < len(COLUMN_MAP):
      order_col_name = COLUMN_MAP[int(order_column_index)]
      order_sql = f" ORDER BY \"{order_col_name}\" {order_dir}"
    else:
        order_sql = f" ORDER BY \"总序号\" ASC"

    # 6. 确定分页
    limit_sql = f" LIMIT {length} OFFSET {start}"
   
    # 7. 获取数据切片
    select_columns = ', '.join([f'"{col}"' for col in COLUMN_MAP])
    data_query = f"SELECT {select_columns} FROM \"{TABLE_NAME}\"{where_sql}{order_sql}{limit_sql}"
   
    df_data_slice = pd.read_sql_query(data_query, db_conn, params=params)
    data_list = df_data_slice.to_dict('records')

    # 8. 返回 DataTables JSON 响应
    return jsonify({
      "draw": draw,
      "recordsTotal": records_total,
      "recordsFiltered": records_filtered,
      "data": data_list
    })

  except Exception as e:
    logging.exception("Server-side DataTables query critical error occurred:")
    return jsonify({'message': f'服务器端处理发生严重错误: {e}'}), 500


@app.route('/api/exportdata', methods=['POST'])
def export_data():
  """获取当前筛选条件下的所有数据，用于前端导出 Excel。"""
  auth_key = request.headers.get('X-Auth-Key')
  if auth_key != AUTH_KEY or not db_conn:
    return jsonify({'message': 'Authorization required or database not ready'}), 401
  try:
    data = request.get_json()
    custom_filters = data.get('custom_filters', {})
    # 忽略 DataTables 的 search_value, start, length
    where_sql, params = build_where_clause(custom_filters, search_value="") 
    select_columns = ', '.join([f'"{col}"' for col in COLUMN_MAP])
    data_query = f"SELECT {select_columns} FROM \"{TABLE_NAME}\"{where_sql}"
    df_export = pd.read_sql_query(data_query, db_conn, params=params)
    export_list = df_export.to_dict('records')
    return jsonify({'data': export_list, 'count': len(export_list)})
  except Exception as e:
    logging.error(f"API 导出数据错误: {e}")
    return jsonify({'message': '服务器遇到数据库错误。'}), 500


if __name__ == '__main__':
  app.run(debug=True, host='0.0.0.0', port=5000)
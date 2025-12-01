import sqlite3
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import atexit
import logging
import shutil  # 用于文件备份操作

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

# 列名映射 (已包含 "查询用车组号")
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
    "市域车所属线路", "特殊涂装", "备注", "座椅类型", "查询用车组号"
]

# 筛选器键到 SQL 列的映射
FILTER_COL_MAP = {
    'attr': '运用属性', 'bureau': '配属局', 'depot': '配属段', 'location': '配属所',
    'model': '车型', 'factory': '制造厂', 'car_count': '编组（辆）'
}

def get_db_connection():
  """返回一个新的 SQLite 连接。"""
  return sqlite3.connect(DB_FILE, check_same_thread=False)

# ======================================================
# 数据库核心逻辑
# ======================================================

def reload_database_from_csv():
    """强制从 CSV 文件重新加载数据到 SQLite"""
    logging.info(f"正在重新加载数据: {CSV_FILE} -> {DB_FILE}...")
    try:
        # 读取 CSV
        df = pd.read_csv(CSV_FILE, encoding='utf-8', skipinitialspace=True, engine='python')
        df.columns = df.columns.str.strip()
        
        # 使用独立的连接进行写入，避免干扰主连接
        # if_exists='replace' 会自动重建表结构
        with sqlite3.connect(DB_FILE) as temp_conn:
            df.to_sql(TABLE_NAME, temp_conn, if_exists='replace', index=False)
            temp_conn.commit()
            
        logging.info(f"数据库重载成功，当前行数: {len(df)}")
        return True, len(df)
    except Exception as e:
        logging.error(f"数据库重载失败: {e}")
        return False, str(e)

def init_sqlite_db():
    """启动时初始化：仅当数据库不存在时才加载，提高启动速度"""
    global db_conn
    if not os.path.exists(DB_FILE):
        logging.info("数据库不存在，执行初始化...")
        reload_database_from_csv()
    else:
        logging.info(f"数据库 {DB_FILE} 已存在，跳过初始化。")
    
    # 建立全局连接（如果尚未建立或已关闭）
    try:
        if db_conn is None:
            db_conn = get_db_connection()
    except Exception:
        db_conn = get_db_connection()

def sync_db_to_csv():
    """辅助函数：将当前 SQLite 数据库状态保存回 CSV 文件 (持久化修改)"""
    try:
        # 读取完整表数据
        df = pd.read_sql_query(f"SELECT * FROM \"{TABLE_NAME}\"", db_conn)
        # 写入 CSV
        df.to_csv(CSV_FILE, index=False, encoding='utf-8')
        logging.info(f"已同步数据库更改到 {CSV_FILE}")
        return True
    except Exception as e:
        logging.error(f"同步 CSV 失败: {e}")
        return False

@atexit.register
def close_db_connection():
  if db_conn:
    db_conn.close()
    logging.info("数据库连接已关闭。")

with app.app_context():
  init_sqlite_db()


# ======================================================
# 路由接口
# ======================================================

@app.route('/api/login', methods=['POST'])
def login():
  """用户登录验证 (模拟)"""
  data = request.get_json()
  password = data.get('password')
  if password == API_PASSWORD:
    return jsonify({'message': 'Login successful', 'auth_key': AUTH_KEY}), 200
  else:
    return jsonify({'message': 'Invalid credentials'}), 401


@app.route('/api/upload_data', methods=['POST'])
def upload_data():
    """上传新的 CSV 文件并刷新数据库"""
    # 1. 验证权限
    auth_key = request.headers.get('X-Auth-Key')
    if auth_key != AUTH_KEY:
        return jsonify({'message': 'Authorization required'}), 401

    # 2. 验证文件
    if 'file' not in request.files:
        return jsonify({'message': '没有上传文件'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'message': '未选择文件'}), 400

    if file:
        try:
            # 3. 保存文件 (覆盖原有 traindata.csv)
            # 建议先备份旧文件
            if os.path.exists(CSV_FILE):
                shutil.copy(CSV_FILE, CSV_FILE + '.bak')
            
            file.save(CSV_FILE)
            
            # 4. 重新加载数据库
            success, info = reload_database_from_csv()
            
            if success:
                return jsonify({'message': f'上传成功！数据库已更新，共 {info} 条记录。'}), 200
            else:
                return jsonify({'message': f'文件保存成功但数据库更新失败: {info}'}), 500
                
        except Exception as e:
            logging.error(f"Upload failed: {e}")
            return jsonify({'message': f'服务器内部错误: {str(e)}'}), 500


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
  """根据自定义筛选器和 DataTables 搜索值构建 SQL WHERE 子句和参数列表。"""
  where_clauses = []
  params = []
 
  # 1. 应用多选筛选器
  for key, col in FILTER_COL_MAP.items():
    values = custom_filters.get(key)
    clean_values = [v for v in values if v is not None and str(v).strip() != ''] if isinstance(values, list) else []
    
    if clean_values:
      placeholders = ', '.join(['?' for _ in clean_values])
      where_clauses.append(f"\"{col}\" IN ({placeholders})")
      params.extend(clean_values)
   
  # 2. 应用自定义 'train' 筛选
  train_search = custom_filters.get('train')
  if train_search and str(train_search).strip() != '':
    normalized_train_search = train_search.replace('-', '').strip()
    train_search_pattern = f'%{normalized_train_search}%'
    where_clauses.append(f"(\"查询用车组号\" LIKE ?)")
    params.append(train_search_pattern)

  # 3. 应用 DataTables 全局搜索
  if search_value and str(search_value).strip() != '':
    normalized_search_value = search_value.replace('-', '').strip()
    search_pattern = f'%{normalized_search_value}%'
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
    data = request.get_json()
    if data is None:
        logging.error("request.get_json() returned None.")
        return jsonify({'message': '请求数据无效，请检查Content-Type或JSON格式。'}), 400 
        
    draw = int(data.get('draw', 0))
    start = int(data.get('start', 0))
    length = int(data.get('length', 25))
    search_value = data.get('search', {}).get('value', '')
    
    order_list = data.get('order', [])
    order_data = order_list[0] if order_list else {}
    order_column_index = order_data.get('column')
    order_dir = order_data.get('dir', 'asc')

    custom_filters = data.get('custom_filters', {})
    where_sql, params = build_where_clause(custom_filters, search_value)
    
    logging.info(f"Filters - Custom: {custom_filters}, Global Search: '{search_value}'")
    
    cursor = db_conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM \"{TABLE_NAME}\"")
    records_total = cursor.fetchone()[0]

    count_query = f"SELECT COUNT(*) FROM \"{TABLE_NAME}\"{where_sql}"
    cursor.execute(count_query, params)
    records_filtered = cursor.fetchone()[0]
    
    # --- 排序逻辑修正：强制按总序号的数字值排序 ---
    order_sql = ""
    if order_column_index is not None and order_column_index != '' and int(order_column_index) < len(COLUMN_MAP):
      order_col_name = COLUMN_MAP[int(order_column_index)]
      
      # 如果排序列是 '总序号'，强制转为整数
      if order_col_name == '总序号':
          order_sql = f" ORDER BY CAST(\"{order_col_name}\" AS INTEGER) {order_dir}"
      else:
          order_sql = f" ORDER BY \"{order_col_name}\" {order_dir}"
    else:
        # 默认排序也是数字顺序
        order_sql = f" ORDER BY CAST(\"总序号\" AS INTEGER) ASC"

    limit_sql = f" LIMIT {length} OFFSET {start}"
    select_columns = ', '.join([f'"{col}"' for col in COLUMN_MAP])
    data_query = f"SELECT {select_columns} FROM \"{TABLE_NAME}\"{where_sql}{order_sql}{limit_sql}"
   
    df_data_slice = pd.read_sql_query(data_query, db_conn, params=params)
    data_list = df_data_slice.to_dict('records')

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
    where_sql, params = build_where_clause(custom_filters, search_value="") 
    select_columns = ', '.join([f'"{col}"' for col in COLUMN_MAP])
    data_query = f"SELECT {select_columns} FROM \"{TABLE_NAME}\"{where_sql}"
    df_export = pd.read_sql_query(data_query, db_conn, params=params)
    export_list = df_export.to_dict('records')
    return jsonify({'data': export_list, 'count': len(export_list)})
  except Exception as e:
    logging.error(f"API 导出数据错误: {e}")
    return jsonify({'message': '服务器遇到数据库错误。'}), 500

# ======================================================
# 后台数据管理 API (增删改)
# ======================================================

@app.route('/api/manage/update', methods=['POST'])
def update_row():
    """更新单行数据"""
    auth_key = request.headers.get('X-Auth-Key')
    if auth_key != AUTH_KEY: return jsonify({'message': 'Auth failed'}), 401

    data = request.get_json()
    row_id = data.get('id') # 总序号
    updates = data.get('data') # 字典

    if not row_id or not updates:
        return jsonify({'message': '缺少参数'}), 400

    try:
        # 构建 UPDATE 语句
        set_clause = []
        params = []
        for col, val in updates.items():
            if col in COLUMN_MAP and col != "总序号": # 不允许修改 ID
                set_clause.append(f"\"{col}\" = ?")
                params.append(val)
        
        if not set_clause:
            return jsonify({'message': '没有有效字段需要更新'}), 400

        params.append(row_id)
        sql = f"UPDATE \"{TABLE_NAME}\" SET {', '.join(set_clause)} WHERE \"总序号\" = ?"
        
        cursor = db_conn.cursor()
        cursor.execute(sql, params)
        db_conn.commit()
        
        # 同步回 CSV
        sync_db_to_csv()
        
        return jsonify({'message': '更新成功'}), 200
    except Exception as e:
        logging.error(f"Update error: {e}")
        return jsonify({'message': str(e)}), 500

@app.route('/api/manage/add', methods=['POST'])
def add_row():
    """新增数据"""
    auth_key = request.headers.get('X-Auth-Key')
    if auth_key != AUTH_KEY: return jsonify({'message': 'Auth failed'}), 401
    
    data = request.get_json()
    row_data = data.get('data')
    
    try:
        # 自动计算新的总序号 (数字类型)
        cursor = db_conn.cursor()
        cursor.execute(f"SELECT MAX(CAST(\"总序号\" AS INTEGER)) FROM \"{TABLE_NAME}\"")
        max_id_row = cursor.fetchone()
        max_id = max_id_row[0] if max_id_row and max_id_row[0] is not None else 0
        new_id = int(max_id) + 1
        
        cols = ["总序号"]
        vals = [new_id]
        placeholders = ["?"]
        
        for col, val in row_data.items():
            if col in COLUMN_MAP:
                cols.append(f"\"{col}\"")
                vals.append(val)
                placeholders.append("?")
        
        sql = f"INSERT INTO \"{TABLE_NAME}\" ({', '.join(cols)}) VALUES ({', '.join(placeholders)})"
        cursor.execute(sql, vals)
        db_conn.commit()
        
        sync_db_to_csv()
        return jsonify({'message': '新增成功', 'new_id': new_id}), 200
    except Exception as e:
        logging.error(f"Add error: {e}")
        return jsonify({'message': str(e)}), 500

@app.route('/api/manage/delete', methods=['POST'])
def delete_row():
    """删除数据"""
    auth_key = request.headers.get('X-Auth-Key')
    if auth_key != AUTH_KEY: return jsonify({'message': 'Auth failed'}), 401
    
    data = request.get_json()
    row_id = data.get('id')
    
    try:
        cursor = db_conn.cursor()
        cursor.execute(f"DELETE FROM \"{TABLE_NAME}\" WHERE \"总序号\" = ?", (row_id,))
        db_conn.commit()
        
        sync_db_to_csv()
        return jsonify({'message': '删除成功'}), 200
    except Exception as e:
        logging.error(f"Delete error: {e}")
        return jsonify({'message': str(e)}), 500

if __name__ == '__main__':
  app.run(debug=True, host='0.0.0.0', port=5000)

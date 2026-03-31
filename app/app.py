from flask import Flask, request, jsonify
import psycopg2, redis, json, os, time

app = Flask(__name__)

redis_client = redis.Redis(
    host=os.environ.get('REDIS_HOST', 'redis'),
    port=6379, decode_responses=True
)

def get_db():
    retries = 5
    while retries > 0:
        try:
            conn = psycopg2.connect(
                host=os.environ.get('DB_HOST','db'),
                database=os.environ.get('DB_NAME','tasks'),
                user=os.environ.get('DB_USER','admin'),
                password=os.environ.get('DB_PASS','admin123')
            )
            return conn
        except Exception as e:
            print("DB not ready, retrying...", e)
            retries -= 1
            time.sleep(2)

    raise Exception("Database connection failed")


def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS tasks(
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            done BOOLEAN DEFAULT FALSE
        )
    ''')
    conn.commit()
    conn.close()

init_db()
@app.route('/tasks', methods=['GET'])
def get_tasks():
    cached = redis_client.get('all_tasks')
    if cached:
        return jsonify(json.loads(cached))

    conn = get_db()
    cur = conn.cursor()
    cur.execute('SELECT id,title,done FROM tasks ORDER BY id')
    tasks = [{'id':r[0],'title':r[1],'done':r[2]} for r in cur.fetchall()]
    conn.close()

    redis_client.setex('all_tasks', 30, json.dumps(tasks))
    return jsonify(tasks)


@app.route('/tasks', methods=['POST'])
def create_task():
    data = request.get_json()
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        'INSERT INTO tasks(title,done) VALUES(%s,%s) RETURNING id',
        (data['title'], False)
    )
    tid = cur.fetchone()[0]
    conn.commit()
    conn.close()

    redis_client.delete('all_tasks')
    return jsonify({'id':tid,'title':data['title'],'done':False}), 201


@app.route('/tasks/<int:task_id>', methods=['DELETE'])
def delete_task(task_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('DELETE FROM tasks WHERE id=%s RETURNING id', (task_id,))
    d = cur.fetchone()
    conn.commit()
    conn.close()

    if not d:
        return jsonify({'error':'not found'}), 404

    redis_client.delete('all_tasks')
    return jsonify({'message':f'Task {task_id} deleted'})


@app.route('/health')
def health():
    v = redis_client.incr('visits')
    return jsonify({'status':'ok','visits':int(v)})


if __name__ == '__main__':
   
    app.run(host='0.0.0.0', port=5000)
cd ~/feathr/registry/sql-registry
export API_BASE="api/v1"
export CONNECTION_STR='dbname=feathr user=feathr password=feathr0501'
uvicorn main:app --host 0.0.0.0 --port 8000 1>sql-registry.log 2>&1 &


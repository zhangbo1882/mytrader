## Run 
always run in virtual environment
```
source .venv/bin/activate
```

### frontend
When you want to start the frontend, you should run
```
./scripts/start_frontend.sh
```
You should save log to ./logs/frontend.log
You can access via http://localhost:5002
### backend
When you want to start the backend or api server, you should run
```
./scripts/start_web.sh
```
You should save log to ./logs/api.log
You can access via http://localhost:5001
### worker
When you want to start the worker, you should run
```
python ./scripts/start_worker.py
```
You should save log to ./logs/worker.log
## Browser Automation
Use `agent-browser` for web automation. Run `agent-browser --help` for all commands.
Core workflow:
1. `agent-browser open <url>` - Navigate to page
2. `agent-browser snapshot -i` - Get interactive elements with refs (@e1, @e2)
3. `agent-browser click @e1` / `fill @e2 "text"` - Interact using refs
4. Re-snapshot after page changes

## Test
You must run the browse automation after you complete the implementation.

## Doc
Do not create extras document except the README.

## operation
When you want to do any operations, use the existing API as much as possible via http://127.0.0.1:5001/api
Refer to web/restx_api.py and web/restx_namespaces.py. 



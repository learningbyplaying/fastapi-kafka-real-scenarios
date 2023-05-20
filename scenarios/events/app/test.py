from fastapi import FastAPI, BackgroundTasks

app = FastAPI()

def continuous_task():
    while True:
        # Perform your desired actions or tasks here
        print("Executing loop iteration...")

@app.get("/")
async def root(background_tasks: BackgroundTasks):
    background_tasks.add_task(continuous_task)
    return {"message": "Task started."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, port=80)

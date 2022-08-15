#简单的测试接口统计


### 性能指标回调写入接口
```markdown
request 
      POST 'http://127.0.0.1:8000/v1/indicators/23424132sadfsaf?query_id=f57ccf4d-37da-4e3c-bd37-3443a8261d86'

body
        {
            "task_id": 1,
            "task_uuid": "23424132sadfsaf",
            "created_at": "2022-03-26T03:45:54",
            "deleted_at": null,
            "written_rows": 0,
            "result_rows": null,
            "duration_ms": 43,
            "query_end_time": "2022-03-24T11:17:54",
            "connection_id": 1,
            "task_type": "SQL",
            "analysis_id": 1,
            "updated_at": "2022-03-28T13:07:31",
            "read_rows": 1,
            "memory_use": 1112.0,
            "query_id": null,
            "query_state": "END"
        }

```

### 对前端开放接口

```markdown
request   GET 'http://127.0.0.1:8000/v1/indicators/1'

response 
        {
            "code": 0,
            "data": {
                "task_id": 1,
                "task_uuid": "23424132sadfsaf",
                "created_at": "2022-03-26T03:45:54",
                "deleted_at": null,
                "written_rows": 0,
                "result_rows": null,
                "duration_ms": 43,
                "query_end_time": "2022-03-24T11:17:54",
                "connection_id": 1,
                "task_type": "SQL",
                "analysis_id": 1,
                "updated_at": "2022-03-28T13:07:31",
                "read_rows": 1,
                "memory_use": 1112.0,
                "query_id": null,
                "query_state": "END"
            },
            "msg": null
        }

```
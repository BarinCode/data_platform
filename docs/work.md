## 作业管理服务 RESTful API 文档

### Schema
1. 所有API通过内网http访问，接收和发送的数据格式统一为JSON
```
Accept: application/json
```

2. 成功/失败响应体
```json
// 成功响应体
{
  "code": 0,
  "data": {
    "foo": ["example_string1", "example_string2", 3, 4],
    "bar": {
      "key": "value",
      "key": "value"
    }
  }
}

// 失败响应体
{
  "code": 400100,
  "msg": "error detail"
}
```
3. 具体的业务错误码对应表（待更新）

| 错误码 | 错误详情 | 
| :--- | :--- |
| 400100 | 业务错误示例 |

4. 空白字段作为`null`返回，不会省略 

5. 所有时间戳均以 UTC time(ISO 8601)格式返回
```sh
YYYY-MM-DDTHH:MM:SSZ
```

### 接口鉴权
无

### 0. 作业创建
```sh
POST /api/v1/works
```

#### 请求体
| 参数名 | 必选 | 类型 | 说明 |
| :--- |:--- |:--- | :--- |
| user_id | 是 | string | 用户ID，由`用户管理`提供 |
| name | 是 | string | 作业名称 |
| description | 是 | string | 作业描述 |
| type | 是 | string | 作业类型, `STREAMING`/`BATCH` |
| category | 是 | string | 作业分类,`FLINK_SQL`/`FLNK_JAR` |
| status | 否 | int | 作业状态 |
| work_directory | 否 | string | 工作目录 |
| main_class_name | 否 | string | 运行主类名称 |
| extra_params | 否 | string | 额外参数 |
| java_version | 否 | string | JAVA版本 |
| run_frequency | 否 | string | 运行频次, once/hourly/daily/weekly |
| created_at | 否 | string | 创建时间 |
| updated_at | 否 | string | 更新时间 |
| deleted_at | 否 | string | 删除时间 |

### 1. 查看作业详情
```sh
GET /api/v1/works/:work_id
```

#### 路径参数
| 参数名 | 必选 | 类型 | 说明 |
| :--- |:--- |:--- | :--- |
| work_id | 是 | string | 作业ID |

#### 响应体示例
```json
{
  "code": 0,
  "data": {
    "work_id": "002588b939b34699a278f7362c4313fa",
    "lambda_id": "0030ea4c962541da80e633e72a22caec",
    "user_id": "001a619f966644ff886976fbdb637fcb",
    "name": "测试作业",
    "description": "一个测试作业",
    "status": 1,
    "work_directory": "/001a619f966644ff886976fbdb637fcb",
    "main_class_name": "Main",
    "extra_params": ["-Doptions fdfdf", ],
    "java_version": "8",
    "run_frequency": "hourly",
    "created_at": "2021-12-01T16:53:34Z",
    "updated_at": "2021-12-01T16:53:34Z",
    "deleted_at": null
  }
}
```

### 2. 更新作业
```sh
PATCH /api/v1/works/:work_id
```

#### 路径参数
| 参数名 | 必选 | 类型 | 说明 |
| :--- |:--- |:--- | :--- |
| work_id | 是 | string | 作业ID |

#### 请求体
| 参数名 | 必选 | 类型 | 说明 |
| :--- |:--- |:--- | :--- |
| user_id | 否 | string | 用户ID，由`用户管理`提供 |
| name | 否 | string | 作业名称 |
| description | 否 | string | 作业描述 |
| status | 否 | int | 作业状态, 0-7 按照原型顺序 |
| work_directory | 否 | string | 工作目录 |
| main_class_name | 否 | string | 运行主类名称 |
| extra_params | 否 | string | 额外参数 |
| java_version | 否 | string | JAVA版本 |
| run_frequency | 否 | string | 运行频次 |

#### 请求体示例
```json
{
  "name": "测试作业2",
  "description": "一个测试作业",
  "status": 2,
  "work_directory": "/001a619f966644ff886976fbdb637fcb",
  "main_class_name": "Main",
  "extra_params": ["-Doptions fdfdf", ],
  "java_version": "8",
  "run_frequency": "hourly",
}
```

#### 响应体
```json
{
  "code": 0,
  "data": {
    "work_id": "002588b939b34699a278f7362c4313fa",
    "lambda_id": "0030ea4c962541da80e633e72a22caec",
    "user_id": "001a619f966644ff886976fbdb637fcb",
    "name": "测试作业2",
    "description": "一个测试作业",
    "status": 1,
    "work_directory": "/001a619f966644ff886976fbdb637fcb",
    "main_class_name": "Main",
    "extra_params": ["-Doptions fdfdf", ],
    "java_version": "8",
    "run_frequency": "hourly",
    "created_at": "2021-12-01T16:53:34Z",
    "updated_at": "2021-12-01T16:53:34Z",
    "deleted_at": null
  }
}
```

### 3. 删除作业
```sh
DELETE /api/v1/works/:work_id
```

#### 路径参数
| 参数名 | 必选 | 类型 | 说明 |
| :--- |:--- |:--- | :--- |
| work_id | 是 | string | 作业ID |

#### 响应体
```json
{
  "code": 0
}
```

### 4. 获取作业列表
```sh
GET /api/v1/works
```

#### 查询参数
| 参数名 | 必选 | 类型 | 说明 |
| :--- |:--- |:--- | :--- |
| limit | 是 | string | 每页数量, 默认`10`|
| offset | 是 | string | 页数 |
| user_id | 否 | string | 用户ID，由`用户管理`提供 |
| name | 否 | string | 作业名称 |
| description | 否 | string | 作业描述 |
| status | 否 | int | 作业状态, 0-5 `TODO` |
| run_frequency | 否 | string | 运行频次 |

TODO order

#### 响应体
```json
{
  "code": 0,
  "data": [
    {
      "work_id": "002588b939b34699a278f7362c4313fa",
      "lambda_id": "0030ea4c962541da80e633e72a22caec",
      "user_id": "001a619f966644ff886976fbdb637fcb",
      "name": "测试作业2",
      "description": "一个测试作业",
      "status": 1,
      "work_directory": "/001a619f966644ff886976fbdb637fcb",
      "main_class_name": "Main",
      "extra_params": ["-Doptions fdfdf", ],
      "java_version": "8",
      "run_frequency": "hourly",
      "created_at": "2021-12-01T16:53:34Z",
      "updated_at": "2021-12-01T16:53:34Z",
      "deleted_at": null
    },
    {
      "work_id": "002588b939b34699a278f7362c4313fa",
      "lambda_id": "0030ea4c962541da80e633e72a22caec",
      "user_id": "001a619f966644ff886976fbdb637fcb",
      "name": "测试作业3",
      "description": "一个测试作业",
      "status": 1,
      "work_directory": "/001a619f966644ff886976fbdb637fcb",
      "main_class_name": "Main",
      "extra_params": ["-Doptions fdfdf", ],
      "java_version": "8",
      "run_frequency": "daily",
      "created_at": "2021-12-01T16:53:34Z",
      "updated_at": "2021-12-01T16:53:34Z",
      "deleted_at": null
    }
  ]
}
```

### 5. 搜索作业
```sh
GET /api/v1/works/search
```

#### 查询参数
| 参数名 | 必选 | 类型 | 说明 |
| :--- |:--- |:--- | :--- |
| limit | 是 | string | 每页数量 |
| offset | 是 | string | 页数 |
| user_id | 否 | string | 用户ID，由`用户管理`提供 |
| name | 否 | string | 作业名称 |
| description | 否 | string | 作业描述 |
| status | 否 | string | 作业状态, 0-5 `TODO` |
| run_frequency | 否 | string | 运行频次 |

#### 响应体
```json
{
  "code": 0,
  "data": [
    {
      "work_id": "002588b939b34699a278f7362c4313fa",
      "lambda_id": "0030ea4c962541da80e633e72a22caec",
      "user_id": "001a619f966644ff886976fbdb637fcb",
      "name": "测试作业2",
      "description": "一个测试作业",
      "status": 1,
      "work_directory": "/001a619f966644ff886976fbdb637fcb",
      "main_class_name": "Main",
      "extra_params": ["-Doptions fdfdf", ],
      "java_version": "8",
      "run_frequency": "hourly",
      "created_at": "2021-12-01T16:53:34Z",
      "updated_at": "2021-12-01T16:53:34Z",
      "deleted_at": null
    },
    {
      "work_id": "002588b939b34699a278f7362c4313fa",
      "lambda_id": "0030ea4c962541da80e633e72a22caec",
      "user_id": "001a619f966644ff886976fbdb637fcb",
      "name": "测试作业3",
      "description": "一个测试作业",
      "status": 1,
      "work_directory": "/001a619f966644ff886976fbdb637fcb",
      "main_class_name": "Main",
      "extra_params": ["-Doptions fdfdf", ],
      "java_version": "8",
      "run_frequency": "daily",
      "created_at": "2021-12-01T16:53:34Z",
      "updated_at": "2021-12-01T16:53:34Z",
      "deleted_at": null
    }
  ]
}
```

### 6. 获取任务列表
```sh
GET /api/v1/works/:work_id/tasks
```

#### 请求体
| 参数名 | 必选 | 类型 | 说明 |
| :--- |:--- |:--- | :--- |
| work_id | 否 | string | 作业ID |

#### 请求体示例
```json
{
  "work_id": "001a619f966644ff886976fbdb637fcb",
}
```

#### 响应体示例
```json
{
  "code": 0,
  "data": [
    {
      "task_id": "005036c4b6ba4ebfab9dc4b26895b6f9",
      "work_id": "001a619f966644ff886976fbdb637fcb",
      "status": 3,
      "started_at": "2021-12-01T16:53:34Z",
      "ended_at": null,
      "run_time": 0
    },
    {
      "task_id": "005036c4b6ba4ebfab9dc4b26895b6g0",
      "work_id": "001a619f966644ff886976fbdb637fcb",
      "status": 0,
      "started_at": "2021-12-01T16:53:34Z",
      "ended_at": "2021-12-01T17:53:34Z",
      "run_time": 3600
    }
  ]
}
```

### 7. 获取任务详情
TODO
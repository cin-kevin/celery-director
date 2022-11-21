{
    "example.WORKFLOW": {
        "tasks": [
            "TASK_EXAMPLE"
        ]
    },
    "example.SIMPLE_CHAIN": {
        "tasks": [
            "TASK_A",
            "TASK_B",
            "TASK_C"
        ]
    },
    "example.SIMPLE_GROUP": {
        "tasks": [
            "TASK_A",
            {
                "EXAMPLE_GROUP": {
                    "type": "group",
                    "tasks": [
                        "TASK_B",
                        "TASK_C"
                    ]
                }
            }
        ]
    },
    "example.ERROR": {
        "tasks": [
            "TASK_ERROR"
        ]
    },
    "example.SIMPLE_CHAIN_ERROR": {
        "tasks": [
            "TASK_A",
            "TASK_B",
            "TASK_ERROR"
        ]
    },
    "example.SIMPLE_GROUP_ERROR": {
        "tasks": [
            "TASK_A",
            {
                "EXAMPLE_GROUP": {
                    "type": "group",
                    "tasks": [
                        "TASK_ERROR",
                        "TASK_C"
                    ]
                }
            }
        ]
    },
    "schemas.SIMPLE_SCHEMA": {
        "tasks": [
            "TASK_EXAMPLE"
        ],
        "schema": "example/simple_schema"
    },
    "example.CELERY_ERROR_ONE_TASK": {
        "tasks": [
            "TASK_CELERY_ERROR"
        ]
    },
    "example.CELERY_ERROR_MULTIPLE_TASKS": {
        "tasks": [
            "TASK_A",
            "TASK_CELERY_ERROR"
        ]
    },
    "example.RETURN_VALUES": {
        "tasks": [
            "STR",
            "INT",
            "LIST",
            "NONE",
            "DICT",
            "NESTED"
        ]
    },
    "example.RETURN_EXCEPTION": {
        "tasks": [
            "STR",
            "TASK_ERROR"
        ]
    },
    "example.TASK_ROUTING": {
        "tasks": [
            "TASK_A",
            {
                "EXAMPLE_GROUP": {
                    "type": "group",
                    "tasks": [
                        "TASK_B",
                        "TASK_C"
                    ]
                }
            }
        ],
        "queue": {
            "default": "q1",
            "customs": {
                "TASK_B": "q2"
            }
        }
    },
    "example.NESTED_WORKFLOW": {
        "type": "chain",
        "tasks": [
            "TASK_A",
            {
                "EXAMPLE_GROUP": {
                    "type": "group",
                    "tasks": [
                        {
                            "TASK_B": {
                                "type": "chain",
                                "tasks": [
                                    "TASK_1",
                                    "TASK_2"
                                ]
                            }
                        },
                        "TASK_C"
                    ]
                }
            },
            "TASK_D"
        ]
    },
    "example.NESTED_WORKFLOW_2_GROUP_CHAINED": {
        "type": "chain",
        "tasks": [
            "TASK_A",
            {
                "EXAMPLE_GROUP": {
                    "type": "group",
                    "tasks": [
                        {
                            "TASK_B": {
                                "type": "chain",
                                "tasks": [
                                    "TASK_1",
                                    "TASK_2"
                                ]
                            }
                        },
                        {
                            "TASK_C": {
                                "type": "chain",
                                "tasks": [
                                    "TASK_1",
                                    "TASK_2"
                                ]
                            }
                        }
                    ]
                }
            },
            "TASK_D"
        ]
    },
    "example.NESTED_WORKFLOW_2_MULTI_GROUP": {
        "type": "chain",
        "tasks": [
            "TASK_A",
            {
                "EXAMPLE_GROUP1": {
                    "type": "group",
                    "tasks": [
                        {
                            "TASK_B": {
                                "type": "chain",
                                "tasks": [
                                    "TASK_1",
                                    "TASK_2"
                                ]
                            }
                        },
                        {
                            "TASK_C": {
                                "type": "chain",
                                "tasks": [
                                    "TASK_1",
                                    "TASK_2"
                                ]
                            }
                        }
                    ]
                }
            },
            "TASK_C",
            {
                "EXAMPLE_GROUP2": {
                    "type": "group",
                    "tasks": [
                        {
                            "TASK_B": {
                                "type": "chain",
                                "tasks": [
                                    "TASK_1",
                                    "TASK_2"
                                ]
                            }
                        },
                        {
                            "TASK_C": {
                                "type": "chain",
                                "tasks": [
                                    "TASK_1",
                                    "TASK_2"
                                ]
                            }
                        }
                    ]
                }
            },
            "TASK_D"
        ]
    }
}

def query_1(collection, year, pos):
    # aggregate 쿼리 정의
    pipeline = [
        { '$match': { 'Release': { '$regex': '^'+str(year) } } },
        { '$project': { 'word': '$statistics.word_frequencies' } },
        { '$unwind': { 'path': '$word' } },
        { '$match': {
            'word.word': {
                '$nin': [
                    '너', '내', '나', '그대', '그', '난', '날', '날', '수', '널', '우리', '것', '이', '네', '더', '또', '니', '걸', '해', '때', '못', '젠', '알', '게', '안', '넌', '거', '건', '위'
                ]
            }, 
            'word.pos': pos
        }},
        { '$group': { '_id': '$word.word', 'cnt': { '$sum': 1 } } },
        { '$sort': { 'cnt': -1 } },
        { '$limit': 10 }
    ]
    
    # aggregate 쿼리 실행
    results = collection.aggregate(pipeline)
    return results

def query_2(collection, genre, year, pos):
    # aggregate 쿼리 정의
    pipeline = [
        { '$match': { 'Genre': genre, 'Release': { '$regex': '^'+str(year) } } },
        { '$project': { 'word': '$statistics.word_frequencies', 'Genre': 1 } },
        { '$unwind': { 'path': '$word' } },
        { '$match': {
            'word.word': {
                '$nin': [
                    '너', '내', '나', '그대', '그', '난', '날', '날', '수', '널', '우리', '것', '이', '네', '더', '또', '니', '걸', '해', '때', '못', '젠', '알', '게', '안', '넌', '거', '건', '위'
                ]
            }, 
            'word.pos': pos
        }},
        { '$group': { '_id': '$word.word', 'cnt': { '$sum': 1 } } },
        { '$sort': { 'cnt': -1 } },
        { '$limit': 10 }
    ]
    
    # aggregate 쿼리 실행
    results = collection.aggregate(pipeline)
    return results

def query_3age(collection):
    # aggregate 쿼리 정의
    pipeline = [
        {
            '$match': {
                '$expr': {
                    '$eq': [
                        {
                            '$size': '$Singer'
                        }, 1
                    ]
                }, 
                'LYRIC': {
                    '$exists': True, 
                    '$ne': '', 
                    '$ne': None
                }
            }
        }, {
            '$unwind': {
                'path': '$Singer'
            }
        }, {
            '$match': {
                'Singer.Birth': {
                    '$exists': True, 
                    '$ne': None, 
                    '$ne': '', 
                    '$ne': '-'
                }, 
                'Release': {
                    '$exists': True, 
                    '$ne': None, 
                    '$ne': '', 
                    '$ne': '-'
                }
            }
        }, {
            '$addFields': {
                'age': {
                    '$substr': [
                        '$Singer.Birth', 0, 4
                    ]
                }, 
                'year': {
                    '$substr': [
                        '$Release', 0, 4
                    ]
                }
            }
        }, {
            '$match': {
                'age': {
                    '$regex': '^[0-9]+$'
                }
            }
        }, {
            '$addFields': {
                'ager': {
                    '$floor': {
                        '$divide': [
                            {
                                '$subtract': [
                                    {
                                        '$toInt': {
                                            '$substr': [
                                                '$Release', 0, 4
                                            ]
                                        }
                                    }, {
                                        '$toInt': {
                                            '$substr': [
                                                '$Singer.Birth', 0, 4
                                            ]
                                        }
                                    }
                                ]
                            }, 10
                        ]
                    }
                }
            }
        }, {
            '$group': {
                '_id': '$ager', 
                'Cnt': {
                    '$sum': 1
                }, 
                'SKOR': {
                    '$sum': '$statistics.korRate'
                }, 
                'SENG': {
                    '$sum': '$statistics.engRate'
                }, 
                'SOTR': {
                    '$sum': '$statistics.otehrRate'
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'Ager': '$_id', 
                'DIVKOR': {
                    '$divide': [
                        '$SKOR', '$Cnt'
                    ]
                }, 
                'DIVENG': {
                    '$divide': [
                        '$SENG', '$Cnt'
                    ]
                }, 
                'DIVOTR': {
                    '$divide': [
                        '$SOTR', '$Cnt'
                    ]
                }
            }
        }, {
            '$sort': {
                'Ager': 1
            }
        }
    ]

    # aggregate 쿼리 실행
    results = collection.aggregate(pipeline)
    return results

def query_3year(collection):
    # aggregate 쿼리 정의
    pipeline = [
        {
            '$addFields': {
                'year': {
                    '$substr': [
                        '$Release', 0, 4
                    ]
                }
            }
        }, {
            '$match': {
                'LYRIC': {
                    '$exists': True, 
                    '$ne': '', 
                    '$ne': None
                }, 
                'year': {
                    '$regex': '^[0-9]+$'
                }
            }
        }, {
            '$group': {
                '_id': '$year', 
                'Cnt': {
                    '$sum': 1
                }, 
                'SKOR': {
                    '$sum': '$statistics.korRate'
                }, 
                'SENG': {
                    '$sum': '$statistics.engRate'
                }, 
                'SOTR': {
                    '$sum': '$statistics.otehrRate'
                }
            }
        }, {
            '$sort': {
                '_id': 1
            }
        }, {
            '$project': {
                '_id': 0, 
                'Year': '$_id', 
                'MeanKOR': {
                    '$divide': [
                        '$SKOR', '$Cnt'
                    ]
                }, 
                'MeanENG': {
                    '$divide': [
                        '$SENG', '$Cnt'
                    ]
                }, 
                'MeanOTR': {
                    '$divide': [
                        '$SOTR', '$Cnt'
                    ]
                }
            }
        }
    ]
    
    # aggregate 쿼리 실행
    results = collection.aggregate(pipeline)
    return results

def query_3genre(collection):
    # aggregate 쿼리 정의
    pipeline = [
        {
            '$match': {
                'LYRIC': {
                    '$exists': True, 
                    '$ne': '', 
                    '$ne': None
                }
            }
        }, {
            '$group': {
                '_id': '$Genre', 
                'Cnt': {
                    '$sum': 1
                }, 
                'KorRate': {
                    '$sum': '$statistics.korRate'
                }, 
                'EngRate': {
                    '$sum': '$statistics.engRate'
                }, 
                'OtherRate': {
                    '$sum': '$statistics.otehrRate'
                }
            }
        }, {
            '$sort': {
                '_id': 1
            }
        }, {
            '$project': {
                '_id': 0, 
                'Genre': '$_id', 
                'DIVKOR': {
                    '$divide': [
                        '$KorRate', '$Cnt'
                    ]
                }, 
                'DIVENG': {
                    '$divide': [
                        '$EngRate', '$Cnt'
                    ]
                }, 
                'DIVOTHER': {
                    '$divide': [
                        '$OtherRate', '$Cnt'
                    ]
                }
            }
        }
    ]
    
    # aggregate 쿼리 실행
    results = collection.aggregate(pipeline)
    return results

def query_4(collection):
    # aggregate 쿼리 정의
    pipeline = [
        {
            '$unwind': {
                'path': '$Singer', 
                'includeArrayIndex': 'string', 
                'preserveNullAndEmptyArrays': True
            }
        }, {
            '$match': {
                '$or': [
                    {
                        'Singer.Type': 0
                    }, {
                        'Singer.Type': 1
                    }
                ]
            }
        }, {
            '$facet': {
                'totalCount': [
                    {
                        '$count': 'total'
                    }
                ], 
                'groupedData': [
                    {
                        '$group': {
                            '_id': {
                                'type': '$Singer.Type', 
                                #'gender': '$Singer.Gender', 
                                'genre': '$Genre'
                            }, 
                            'Cnt': {
                                '$sum': 1
                            }
                        }
                    }
                ]
            }
        }, {
            '$unwind': {
                'path': '$totalCount'
            }
        }, {
            '$unwind': {
                'path': '$groupedData'
            }
        }, {
            '$project': {
                'Type': '$groupedData._id.type', 
                #'Gender': '$groupedData._id.gender', 
                'Genre': '$groupedData._id.genre', 
                'Proportion': {
                    '$divide': [
                        '$groupedData.Cnt', '$totalCount.total'
                    ]
                }
            }
        }, {
            '$sort': {
                'Genre': 1, 
                'Type': 1, 
                'Gender': 1
            }
        }
    ]
    
    # aggregate 쿼리 실행
    results = collection.aggregate(pipeline).batch_size(200000)
    return results

def query_5genre(collection):
    # aggregate 쿼리 정의
    pipeline = [
        {
            '$match': {
                'LYRIC': {
                    '$exists': True, 
                    '$ne': '', 
                    '$ne': None
                }
            }
        }, {
            '$group': {
                '_id': '$Genre', 
                'Cnt': {
                    '$sum': 1
                }, 
                'WC': {
                    '$sum': '$statistics.word_count'
                }, 
                'UWC': {
                    '$sum': '$statistics.unique_word_count'
                }, 
                'line': {
                    '$sum': '$statistics.lineCnt'
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'Genre': '$_id', 
                'MEN_word': {
                    '$floor': {
                        '$divide': [
                            '$WC', '$Cnt'
                        ]
                    }
                }, 
                'MEN_uniqe_word': {
                    '$floor': {
                        '$divide': [
                            '$UWC', '$Cnt'
                        ]
                    }
                }, 
                'MEN_Line': {
                    '$floor': {
                        '$divide': [
                            '$line', '$Cnt'
                        ]
                    }
                }
            }
        }
    ]
    
    # aggregate 쿼리 실행
    results = collection.aggregate(pipeline)
    return results

def query_5year(collection):
    # aggregate 쿼리 정의
    pipeline = [
        {
            '$match': {
                'LYRIC': {
                    '$exists': True, 
                    '$ne': '', 
                    '$ne': None
                }
            }
        }, {
            '$addFields': {
                'year': {
                    '$substr': [
                        '$Release', 0, 4
                    ]
                }
            }
        }, {
            '$match': {
                'year': {
                    '$regex': '^[0-9]+$'
                }
            }
        }, {
            '$group': {
                '_id': '$year', 
                'Cnt': {
                    '$sum': 1
                }, 
                'WC': {
                    '$sum': '$statistics.word_count'
                }, 
                'UWC': {
                    '$sum': '$statistics.unique_word_count'
                }, 
                'line': {
                    '$sum': '$statistics.lineCnt'
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'Year': '$_id', 
                'AVGWC': {
                    '$floor': {
                        '$divide': [
                            '$WC', '$Cnt'
                        ]
                    }
                }, 
                'AVGUWC': {
                    '$floor': {
                        '$divide': [
                            '$UWC', '$Cnt'
                        ]
                    }
                }, 
                'AVGLine': {
                    '$floor': {
                        '$divide': [
                            '$line', '$Cnt'
                        ]
                    }
                }
            }
        }, {
            '$sort': {
                'Year': 1
            }
        }
    ]
    
    # aggregate 쿼리 실행
    results = collection.aggregate(pipeline)
    return results

def query_6(collection, company, pos, limit):
    # aggregate 쿼리 정의
    pipeline = [
        {
            '$match': {
                'Singer.Company': {
                    '$in': company
                }
            }
        }, {
            '$project': {
                'word': '$statistics.word_frequencies', 
                'Genre': 1
            }
        }, {
            '$unwind': {
                'path': '$word'
            }
        }, {
            '$match': {
                'word.word': {
                    '$nin': [
                        '너', '내', '나', '그대', '그', '난', '날', '날', '수', '널', '우리', '것', '이', '네', '더', '또', '니', '걸', '해', '때', '못', '젠', '알', '게', '안', '넌', '거', '건', '위'
                    ]
                }, 
                'word.pos': pos
            }
        }, {
            '$group': {
                '_id': '$word.word', 
                'cnt': {
                    '$sum': 1 #'$word.cnt'
                }
            }
        }, {
            '$sort': {
                'cnt': -1
            }
        }, {
            '$limit': limit
        }
    ]
    
    # aggregate 쿼리 실행
    results = collection.aggregate(pipeline)
    return results

def query_7(collection):
    # aggregate 쿼리 정의
    pipeline = [
        {
            '$unwind': {
                'path': '$Singer'
            }
        }, {
            '$unwind': {
                'path': '$Singer.Company'
            }
        }, {
            '$addFields': {
                'companyGroup': {
                    '$switch': {
                        'branches': [
                            {
                                'case': {
                                    '$in': [
                                        {
                                            '$getField': {
                                                'field': 'Company', 
                                                'input': '$Singer'
                                            }
                                        }, [
                                            '(주)SM엔터테인먼트', '키이스트', '미스틱스토리'
                                        ]
                                    ]
                                }, 
                                'then': 'SM'
                            }, {
                                'case': {
                                    '$in': [
                                        {
                                            '$getField': {
                                                'field': 'Company', 
                                                'input': '$Singer'
                                            }
                                        }, [
                                            'YG엔터테인먼트', '(주)YG엔터테인먼트', 'THEBLACKLABEL'
                                        ]
                                    ]
                                }, 
                                'then': 'YG'
                            }, {
                                'case': {
                                    '$in': [
                                        {
                                            '$getField': {
                                                'field': 'Company', 
                                                'input': '$Singer'
                                            }
                                        }, [
                                            '(주)JYP엔터테인먼트'
                                        ]
                                    ]
                                }, 
                                'then': 'JYP'
                            }, {
                                'case': {
                                    '$in': [
                                        {
                                            '$getField': {
                                                'field': 'Company', 
                                                'input': '$Singer'
                                            }
                                        }, [
                                            '빅히트엔터테인먼트', '빌리프랩', '(주)쏘스뮤직', '플레디스', 'KOZ엔터테인먼트', '주식회사어도어'
                                        ]
                                    ]
                                }, 
                                'then': 'HYBE'
                            }
                        ], 
                        'default': 'Others'
                    }
                }
            }
        }, {
            '$group': {
                '_id': '$companyGroup', 
                'likes': {
                    '$sum': '$Like'
                }
            }
        }
    ]
    
    # aggregate 쿼리 실행
    results = collection.aggregate(pipeline)
    return results

def query_8(collection):
    # aggregate 쿼리 정의
    pipeline = [
        {
            '$unwind': {
                'path': '$Singer'
            }
        }, {
            '$unwind': {
                'path': '$Singer.Company'
            }
        }, {
            '$addFields': {
                'companyGroup': {
                    '$switch': {
                        'branches': [
                            {
                                'case': {
                                    '$in': [
                                        {
                                            '$getField': {
                                                'field': 'Company', 
                                                'input': '$Singer'
                                            }
                                        }, [
                                            '(주)SM엔터테인먼트', '키이스트', '미스틱스토리'
                                        ]
                                    ]
                                }, 
                                'then': 'SM'
                            }, {
                                'case': {
                                    '$in': [
                                        {
                                            '$getField': {
                                                'field': 'Company', 
                                                'input': '$Singer'
                                            }
                                        }, [
                                            'YG엔터테인먼트', '(주)YG엔터테인먼트', 'THEBLACKLABEL'
                                        ]
                                    ]
                                }, 
                                'then': 'YG'
                            }, {
                                'case': {
                                    '$in': [
                                        {
                                            '$getField': {
                                                'field': 'Company', 
                                                'input': '$Singer'
                                            }
                                        }, [
                                            '(주)JYP엔터테인먼트'
                                        ]
                                    ]
                                }, 
                                'then': 'JYP'
                            }, {
                                'case': {
                                    '$in': [
                                        {
                                            '$getField': {
                                                'field': 'Company', 
                                                'input': '$Singer'
                                            }
                                        }, [
                                            '빅히트엔터테인먼트', '빌리프랩', '(주)쏘스뮤직', '플레디스', 'KOZ엔터테인먼트', '주식회사어도어'
                                        ]
                                    ]
                                }, 
                                'then': 'HYBE'
                            }
                        ], 
                        'default': 'Others'
                    }
                }
            }
        }, {
            '$group': {
                '_id': '$companyGroup', 
                'totalMusic': {
                    '$sum': 1
                }
            }
        }
    ]
    
    # aggregate 쿼리 실행
    results = collection.aggregate(pipeline)
    return results

def query_9(collection, girlgroups):
    # aggregate 쿼리 정의
    pipeline = [
        {
            '$match': {
                'Singer.0.Name': {
                    '$in': girlgroups
                }, 
                'Singer.1': {
                    '$exists': False
                }
            }
        }, {
            '$group': {
                '_id': '$Singer.Name', 
                'Like': {
                    '$sum': '$Like'
                }, 
                'FirstRelease': {
                    '$min': '$Release'
                }
            }
        }, {
            '$unwind': {
                'path': '$_id'
            }
        }
    ]
    
    # aggregate 쿼리 실행
    results = collection.aggregate(pipeline)
    return results
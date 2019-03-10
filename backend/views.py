#coding=utf8
from django.shortcuts import render
from django.shortcuts import redirect
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt, ensure_csrf_cookie
from time import strftime,gmtime
import random, os, json, queue, pickle, math, sqlite3, re, math, time

db_path = 'searchengine.sqlite' # 数据库路径
page_rank_path = 'page_rank_score.json'
searched = {} # 已查字段
page_rank = None
all_pages = None

@csrf_exempt
def index(request):
    ''' 返回首页 '''
    return render(request, 'index.html')

@csrf_exempt
def redirect(request, params):
    ''''''
    return render(request, 'pages/' + params)

def log(query, used_time):
    with open('query_log.txt', 'a') as f:
        f.write(strftime("%m/%d/%Y %H:%M") + ':\tquery for ' + query + '\tused time: ' + str(used_time) + ' s\n')
        f.close()

@csrf_exempt
def search(request):
    ''' 根据查询表达式进行搜索 '''
    start_time = time.time()
    # 获取page_rank value
    global page_rank, all_pages
    if page_rank is None:
        with open(page_rank_path, 'r') as f:
            pr = json.load(f)
            f.close()
        page_rank = {}
        all_pages = {}
        cursor = get_cursor()
        sql = 'select * from page'
        cursor.execute(sql)
        temp = cursor.fetchall()
        for item in temp:
            # key->url value->page_id
            all_pages[item[2]] = item[0]
        for item in all_pages:
            page_rank[item] = pr[all_pages[item]]
    query = json.loads(request.body.decode('utf8'))
    words = re.split(' |:', query['query'])
    print(query)
    page_num = query['page_num'] # 页面
    if query['query'] in searched:
        # 已查找
        end_time = time.time()
        used_time = round(end_time - start_time, 4)
        log(query['query'], used_time)
        return JsonResponse({
            'msg': process_result(get_from_searched(query['query'], page_num)),
            'total_num': math.ceil(len(searched[query['query']]) / 10),
            'used_time': used_time,
            'type': 'filetype' if 'filetype' in query['query'] else 'normal'
            })
    # 检查搜索类型
    if 'filetype' in words:
        # 查找文件
        split_index = words.index('filetype')
        key_words = words[: split_index]
        types = words[split_index + 1: ]
        res = search_file(key_words, types, query['query'])
        end_time = time.time()
        used_time = round(end_time - start_time, 4)
        log(query['query'], used_time)
        return JsonResponse({
            'msg': res,
            'total_num': math.ceil(len(res) / 10),
            'used_time': used_time,
            'type': 'filetype'
        })
    else:
        # 普通搜索
        res = search_normal(words, query['query'])
        end_time = time.time()
        used_time = round(end_time - start_time, 4)
        log(query['query'], used_time)
        return JsonResponse({
            'msg': process_result(res),
            'total_num': math.ceil(len(searched[query['query']]) / 10),
            'used_time': used_time,
            'type': 'normal'
        })
    
def get_cursor():
    """返回数据库操作器"""
    db = sqlite3.connect(db_path)
    cursor = db.cursor()
    return cursor

def search_file(key_words, types, raw_query=''):
    """文件查找"""
    if len(key_words) == 0:
        return None
    cursor = get_cursor()
    # 在锚文本进行查找
    # 将通配符*用%替换
    search_words = []
    for key_word in key_words:
        search_words.append(key_word.replace('*', '%'))
    sql = 'select * from link where anchor like "%' + search_words[0] + '%"'
    cursor.execute(sql)
    res = cursor.fetchall()
    temp = []
    # 将不符合后面匹配规则的页面删除
    for item in res:
        if item[2] not in all_pages:
            continue
        flag = True
        for i in range(1, len(search_words)):
            word = search_words[i]
            if not re.match(word, item[3]):
                flag = False
                break
        if flag:
            temp.append(item)
    ret = []
    for item in res:
        print(types[1])
        if item[2].endswith(types[1]):
            ret.append({
                'page_url': item[2],
                'title': item[3]
            })
    return ret
    


def search_normal(key_words=[], raw_query=''):
    """短语查找"""
    if len(key_words) == 0:
        return None
    cursor = get_cursor()
    # 在锚文本进行查找
    # 将通配符*用%替换
    search_words = []
    for key_word in key_words:
        search_words.append(key_word.replace('*', '%'))
    sql = 'select * from link where anchor like "%' + search_words[0] + '%"'
    cursor.execute(sql)
    res = cursor.fetchall()
    temp = []
    # 将不符合后面匹配规则的页面删除
    for item in res:
        if item[2] not in all_pages:
            continue
        flag = True
        for i in range(1, len(search_words)):
            word = search_words[i]
            if not re.match(word, item[3]):
                flag = False
                break
        if flag:
            temp.append(item)
    # 排序
    tmp = []
    tmp_url = set()
    for item in temp:
        # print(len(tmp_url))
        if item[2] not in tmp_url:
            tmp.append(item)
            tmp_url.add(item[2])
    temp = sorted(tmp, key=lambda item: page_rank[item[2]], reverse=True)
    # 将temp添加到searched中
    searched[raw_query] = temp
    res = temp[0: min(10, len(temp))]
    # 根据res的url找到对应的pageo
    ret = []
    for item in res:
        temp = get_by_url(item[2])
        if temp is None:
            continue
        ret.append(temp)
    return ret

def get_by_url(url):
    """通过url返回page信息"""
    cursor = get_cursor()
    sql = "select * from page where url = '" + url + "'"
    cursor.execute(sql)
    temp = cursor.fetchall()
    if len(temp) == 0:
        return None
    return temp[0]

def get_from_searched(word, page_num):
    """在内存中查找"""
    temp = searched[word]
    res = temp[page_num * 10: min(page_num * 10 + 10, len(temp))]
    ret = []
    for item in res:
        temp = get_by_url(item[2])
        if temp is None:
            continue
        ret.append(temp)
    return ret

def process_result(res):
    """将python数组转成前端可以读取的"""
    ret = []
    for item in res:
        temp = {
            'page_id': item[0],
            'page_url': item[1],
            'raw_html': item[6],
            'title': item[7]
        }
        ret.append(temp)
    return ret

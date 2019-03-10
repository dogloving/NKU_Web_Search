#coding = utf8
import sqlite3, time, json, os
import numpy as np
from scipy import sparse

class PageRanker:
    page_count = 12000
    db_path = "./searchengine.sqlite"
    matrix_path = "./backend/src/data/matrix.npz"
    page_rank_path = "./backend/src/data/page_rank_score.json"
    log_path = "./page_rank_log.txt"
    alpha = 0.85
    pages_db = None
    matrix = None

    def __init__(self, page_count=0):
        self.pages_db = sqlite3.connect(self.db_path)
        self.page_count = page_count

    @staticmethod
    def vector_diff(vector1, vector2):
        assert len(vector1) == len(vector2)
        diff = 0
        for i in range(0, len(vector1)):
            diff += abs(vector1[i] - vector2[i])
        return diff

    def set_length(self, length):
        self.page_count = length

    def calculate_page_rank(self):
        start_time = time.time()
        global_vector = np.array([1 / float(self.page_count)] * self.page_count)
        last_vector = [0] * self.page_count
        column_vector = np.array([1 / float(self.page_count)] * self.page_count)
        print("Sum of the matrix is " + str(self.matrix.sum()))
        print("Now the sum of scores is " + str(self.vector_sum(column_vector.tolist())))
        times = 0
        while self.vector_diff(last_vector, column_vector) > 1.0e-15:
            iter_start = time.time()
            print(str(times) + " iteration  starts...")
            last_vector = column_vector.copy()
            column_vector = self.matrix.dot(column_vector)
            column_vector *= self.alpha
            column_vector += (1 - self.alpha) * global_vector

            print(str(times) + " iteration finished")
            print("Now the sum of scores is " + str(self.vector_sum(column_vector.tolist())))
            print(str((time.time() - iter_start)) + " sec has been used for this iteration.")
            print(str((time.time() - start_time)) + " sec has been used in total.\n")
            times += 1

        with open(self.page_rank_path, 'w') as f:
            json.dump(column_vector.tolist(), f)
            f.close()

    def get_out_link_urls(self, pageid):
        cursor = self.pages_db.cursor()
        try:
            cursor.execute("select dst_url from link where src_id=?", (str(pageid),))
            pages = cursor.fetchall()
            return pages
        except Exception as e:
            print(e)

    def get_page_id(self, url):
        cursor = self.pages_db.cursor()
        try:
            cursor.execute("select pageid from page where url=? or final_url=?", (url, url))
            result = cursor.fetchone()
            if result is None:
                return None
            else:
                return result[0]
        except Exception as e:
            print(e)

    def log(self, info):
        print(info)
        f = open(self.log_path, 'a')
        f.write(info)
        f.close()

    def load_matrix(self):
        self.matrix = sparse.load_npz(self.matrix_path)
        print(type(self.matrix))

    def build_matrix(self):
        start_time = int(time.time())
        self.matrix = sparse.lil_matrix((self.page_count, self.page_count), dtype='float64')
        for page in range(0, self.page_count):
            out_urls = self.get_out_link_urls(page)
            out_pages = set()
            for url in out_urls:
                page_id = self.get_page_id(url[0])
                if page_id is None:
                    continue
                else:
                    out_pages.add(page_id)
            if len(out_pages) != 0:
                inverse_out_count = float(1.0 / len(out_pages))
                for page_id in out_pages:
                    self.matrix[page_id, page] = inverse_out_count

            if page % 100 == 0:
                print("Column " + str(page) + " constructed!")
                print(
                    str(int((int(time.time()) - start_time) / (page + 1) * (self.page_count - page))) + " sec left!\n")
        self.matrix = self.matrix.tocsr()
        sparse.save_npz(self.matrix_path, self.matrix)
        self.pages_db.close()

    @staticmethod
    def vector_sum(vector):
        score_sum = 0
        for score in vector:
            score_sum += score
        return score_sum

if __name__ == "__main__":
    db_path = "./src/searchengine.sqlite"
    cursor = sqlite3.connect(db_path)
    cursor = cursor.cursor()
    sql = 'select * from page;'
    cursor.execute(sql)
    length = len(cursor.fetchall())
    ranker = PageRanker(length)

    matrix_path = "./src/data/matrix.npz"
    if not os.path.exists(matrix_path):
        ranker.build_matrix()
    else:
        ranker.load_matrix()
    ranker.calculate_page_rank()
    print('page rank计算结束')
    
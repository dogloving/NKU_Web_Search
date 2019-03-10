#coding = utf8
import re, sqlite3
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse


def get_url_path(url):
    return urlparse(url)[2]

base = "<base href=\"{}\">"

class WebPage:
    pageid = int()
    page_url = ""
    real_url = ""
    page_header = {}
    is_text = bool()
    raw_html = ""
    byte_data = None
    title = ""
    charset = ""
    links_and_anchors = []
    class Link:
        page_url = ""
        anchor = ""
        def __init__(self, url, anchor=""):
            self.page_url = url
            self.anchor = anchor
        def get_url(self):
            return self.page_url
        def get_anchor(self):
            return self.anchor
    def __init__(self, response, initiate_url, page_id):
        self.pageid = page_id
        self.page_url = initiate_url
        self.real_url = response.url
        self.page_header = response.headers
        self.byte_data = response.content
        if response.encoding is None:
            self.is_text = False
            suffix_pattern = r"\/([^\/]+)\.[^\/]+$"
            if re.search(suffix_pattern, self.real_url):
                self.title = re.search(suffix_pattern, self.real_url).group(1)
        else:
            self.is_text = True
            if response.encoding == 'ISO-8859-1':
                raw_html = response.text
                charset_re = re.search("charset=[\"\']?(.+?)[\"\']", raw_html)
                if charset_re is not None:
                    self.charset = charset_re.group(1)
                else:
                    self.charset = "utf8"
                try:
                    self.raw_html = self.byte_data.decode(self.charset, 'ignore')
                except Exception as e:
                    print(e)
            else:
                self.charset = response.encoding
                self.raw_html = response.text
            title_search = re.search(r"<title>(.+)</title>", self.raw_html)
            if title_search is not None:
                self.title = title_search.group(1)

    def store(self, db_conn, root_folder):
        # store page info
        if self.is_text:
            is_text = 1
        else:
            is_text = 0
        try:
            cursor = db_conn.cursor()
            cursor.execute('''
            insert into page(pageid, url, final_url, is_text, header_content_type, header_date, raw_html, title)
                values(?, ?, ?, ?, ?, ?, ?, ?)
            ''',(self.pageid, self.page_url, self.real_url, is_text, self.page_header.get("content-type"), self.page_header.get("date"), WebPage.get_text(self.raw_html), self.title))
        except sqlite3.IntegrityError:
            print('ERROR: ID already exists in PRIMARY KEY column {}'.format(self.pageid))
        finally:
            db_conn.commit()
        # store link info
        try:
            cursor = db_conn.cursor()
            for link in self.links_and_anchors:
                cursor.execute('''
                insert into link(linkid, src_id, dst_url, anchor)
                    values(NULL, ?, ?, ?)
                ''',(self.pageid, link.get_url(), link.get_anchor()))
        except Exception as e:
            print(e)
        finally:
            db_conn.commit()

        try:
            if self.is_text:
                with open(root_folder + str(self.pageid) + ".html", "w", encoding=self.charset) as f:
                    f.write(
                        base.format(self.real_url)
                        + self.raw_html
                    )
                    f.close()
            else:
                suffix_re = re.search(r"/[^/]+(\.[^/]+)$", get_url_path(self.real_url))
                if suffix_re:
                    suffix = suffix_re.group(1)
                    with open(root_folder + str(self.pageid) + suffix, "wb") as f:
                        f.write(self.byte_data)
                        f.close()
        except Exception as err:
            print(err)
        finally:
            return

    @staticmethod
    def get_text(raw_html):
        try:
            soup = BeautifulSoup(raw_html)
            for s in soup('script'):
                s.extract()
            for s in soup('style'):
                s.extract()
            modified_html = soup.get_text()
            modified_html = re.sub(r"(\\\w?)|([\s\n\t\r]+)", "", modified_html)
            modified_html = re.sub(r"([\\/.,\"\'()]+)", " ", modified_html)
            return modified_html
        except Exception as err:
            print(raw_html)
            print(err)

    def parse_links_and_anchors(self):
        if self.is_text is False:
            return []
        meta_pattern = r"<meta.+url=(.*?)(?:\"|\')>"
        meta_re = re.compile(meta_pattern)
        js_pattern = r"(?:(?:(?:top\.location)|(?:window\.navigate)|(?:(?:window\.)?self\.location)|(?:(?:window\.)?location\.href))\s*=\s*(?:\'|\")(.+?)(?:\'|\"))"
        js_re = re.compile(js_pattern)
        onclick_pattern = r"<(\w+)[^<]*onClick=\"(?:(?:(?:top\.location)|(?:window\.navigate)|(?:(?:window\.)?self\.location)|(?:(?:window\.)?location\.href))\s*=\s*(?:\'|\")(.+?)(?:\'|\"))\".*?>(.*?)<\/\1>"
        onclick_re = re.compile(onclick_pattern)
        href_js_pattern = r"<(\w+)[^<]*href=\"javascript:(?:(?:(?:top\.location)|(?:window\.navigate)|(?:(?:window\.)?self\.location)|(?:(?:window\.)?location\.href))\s*=\s*(?:\'|\")(.+?)(?:\'|\")).*?>(.*?)<\/\1>"
        href_js_re = re.compile(href_js_pattern)
        href_pattern = r"<(\w+)[^<]*href=\"((?!javascript|#).+?)\".*?>(.*?)<\/\1>"
        href_re = re.compile(href_pattern)
        all_links = [
            meta_re.findall(self.raw_html),
            js_re.findall(self.raw_html),
            onclick_re.findall(self.raw_html),
            href_js_re.findall(self.raw_html),
            href_re.findall(self.raw_html)
        ]
        self.links_and_anchors = []
        for i in range(0, len(all_links)):
            for links in all_links[i]:
                if i <= 1:
                    link = links[0]
                    url = urljoin(self.real_url, link)
                    self.links_and_anchors.append(WebPage.Link(url))
                else:
                    link = links[1]
                    url = urljoin(self.real_url, link)
                    anchor = links[2]
                    if re.search(r"(?:<.*?>)+([^<>]*)(?:</.*?>)+", anchor) is not None:
                        anchor = re.search(r"(?:<.*?>)+([^<>]*)(?:</.*?>)+", anchor).group(1)
                    self.links_and_anchors.append(WebPage.Link(url, WebPage.get_text(anchor)))
        return self.links_and_anchors

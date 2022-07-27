'''
Created in 2022

@author: David Abián
'''

from mwviews.api import PageviewsClient
import re
import requests
import time

class WdMetrics(object):
    '''
    Class to retrieve and append contribution and demand metrics
    corresponding to the Wikidata QIDs that are defined as
    indices of a dataframe.
    '''

    def __init__(self,
                 df,
                 bot_login_name,
                 bot_login_password,
                 timestamp_start,
                 timestamp_end,
                 language_codes,
                 api_url="https://www.wikidata.org/w/api.php",
                 sitelink_request_chunk_size=50,
                 user_agent=None):
        '''
        Constructor
        '''
        qid_regex = r'^Q[1-9][0-9]*$'
        # set, to avoid retrieving data on the same title several times
        all_qids = list(set(df.index))
        for qid in all_qids:
            assert re.match(qid_regex, qid)
        assert len(bot_login_name) > 0
        assert len(bot_login_password) > 0
        date_regex = r'20[12][0-9]-(0[0-9]|1[0-2])-[0-3][0-9]'
        time_regex = r'[0-2][0-9]:[0-5][0-9]:[0-5][0-9]Z'
        timestamp_regex = r'^' + date_regex + r'T' + time_regex + r'$'
        assert re.match(timestamp_regex, timestamp_start)
        assert re.match(timestamp_regex, timestamp_end)
        assert timestamp_start < timestamp_end
        api_url_regex = r'^https?://\S+$'
        assert re.match(api_url_regex, api_url)
        assert sitelink_request_chunk_size > 0
        
        self.df = df
        self.all_qids = all_qids
        self.bot_login_name = bot_login_name
        self.bot_login_password = bot_login_password
        self.timestamp_start = timestamp_start
        self.timestamp_end = timestamp_end
        self.timestamp_start_short = timestamp_start[0:10].replace("-", "")
        self.timestamp_end_short = timestamp_end[0:10].replace("-", "")
        self.years_l = range(int(self.timestamp_start[:4]),
                             int(self.timestamp_end[:4])+1)
        self.years_l = [str(year) for year in self.years_l]
        self.language_codes = language_codes
        self.wikis = [(lang_code + "wiki") for lang_code in language_codes]
        self.api_url = api_url
        self.sitelink_request_chunk_size = sitelink_request_chunk_size
        # user_agent = "<person@organization.org> Description"
        self.pvclient = PageviewsClient(parallelism=4,
                                        user_agent=user_agent)
        self.session = requests.Session()
        
        self.__login()
        self.bots = self.__get_bot_set()
        print(len(self.bots), "bots")
    
    def __login(self):
        params = {
            'action': "query", 
            'meta': "tokens", 
            'type': "login", 
            'format': "json",
        }
        login_response_data = self.__try_request(params)
        login_token = login_response_data['query']['tokens']['logintoken']
        # credentials via https://www.wikidata.org/wiki/Special:BotPasswords
        params = {
            'action': "login", 
            'lgname': self.bot_login_name, 
            'lgpassword': self.bot_login_password, 
            'lgtoken': login_token, 
            'format': "json",
        }
        login_response_data = self.__try_request(params,
                                                 post_method=True)
        print(login_response_data)

    def __get_request_error(self, r):
        return "status code: " + str(r.status_code) + " / " + str(r)

    def __try_request(self, params, post_method=False, check_success_flag=False):
        if post_method:
            r = self.session.post(url=self.api_url, data=params)
        else:
            r = self.session.get(url=self.api_url, params=params)
        if r.status_code != 200:
            raise ValueError(self.__get_request_error(r))
        r_json = r.json()
        if check_success_flag and ("success" not in r_json or r_json["success"] != 1):
            raise ValueError(self.__get_request_error(r))
        return r_json

    def __try_and_retry_request(self, params, post_method=False, check_success_flag=False):
        time_to_retry = 20
        attempts = 4
        waiting_factor = 2.3
        for _ in range(attempts):
            try:
                response = self.__try_request(params,
                                              post_method=post_method,
                                              check_success_flag=check_success_flag)
            except:
                time.sleep(time_to_retry)
                time_to_retry *= waiting_factor
            else:
                break
        else:
            self.__login()
            response = self.__try_request(params,
                                          post_method=post_method,
                                          check_success_flag=check_success_flag)
        return response

    def __append_num_edits_and_activity_days(self):
        for rev_year in self.years_l:
            self.df["edits_" + rev_year] = 0
            self.df["activity_days_" + rev_year] = 0
            self.df["human_editors_" + rev_year] = 0
        for qid in self.all_qids:
            print(qid)
            parameters = {
                'action': 'query',
                'format': 'json', 
                'continue': '', 
                'titles': qid, 
                'prop': 'revisions', 
                'rvprop': 'timestamp|ids|userid|user', 
                'rvlimit': 'max',
            }
            activity_days = {year: set() for year in self.years_l}
            human_editors = {year: set() for year in self.years_l}
            continue_iterating = True
            while continue_iterating:
                response = self.__try_and_retry_request(parameters)
                response_pages = response['query']['pages']
                for page_id in response_pages:
                    if 'revisions' in response_pages[page_id]:
                        for rev in response_pages[page_id]['revisions']:
                            if rev['timestamp'] >= self.timestamp_start and rev['timestamp'] <= self.timestamp_end:
                                rev_year = rev['timestamp'][:4]
                                rev_date = rev['timestamp'][:10]
                                self.df.loc[qid, "edits_" + rev_year] += 1
                                activity_days[rev_year].add(rev_date)
                                if 'user' in rev and rev['user'] not in self.bots:
                                    human_editors[rev_year].add(rev['user'])
                if 'continue' in response:
                    parameters['continue'] = response['continue']['continue']
                    parameters['rvcontinue'] = response['continue']['rvcontinue']
                else:
                    continue_iterating = False
            human_editors_set = set()
            for year in self.years_l:
                self.df.loc[qid, "activity_days_" + year] = len(activity_days[year])
                self.df.loc[qid, "human_editors_" + year] = len(human_editors[year])
                human_editors_set.update(human_editors[year])
            self.df.loc[qid, "human_editors"] = len(human_editors_set)
        self.df["activity_days"] = self.df[["activity_days_" + year for year in self.years_l]].sum(axis=1)
        self.df["edits"] = self.df[["edits_" + year for year in self.years_l]].sum(axis=1)

    def __get_bot_set(self):
        bot_cats = [
            "Category:Bots without botflag",
            "Category:Bots with botflag",
            "Category:Bots running on Wikimedia Toolforge",
            "Category:Extension bots",
        ]
        bot_set = set()
        for bot_cat in bot_cats:
            parameters = {
                "action": "query",
                "cmtitle": bot_cat,
                "cmlimit": "max",
                "list": "categorymembers",
                "format": "json",
            }
            continue_iterating = True
            while continue_iterating:
                response_json = self.__try_and_retry_request(parameters)
                pages = response_json['query']['categorymembers']
                for page in pages:
                    if re.match(r'^User:[^:/]+$', page['title']):
                        bot_set.add(page['title'].replace("User:", ""))
                if "continue" in response_json:
                    parameters["cmcontinue"] = response_json["continue"]["cmcontinue"]
                    parameters["continue"] = response_json["continue"]["continue"]
                else:
                    continue_iterating = False
        parameters = {
            "action": "query",
            "format": "json",
            "list": "allusers",
            "aulimit": "max", # TODO: implement pagination for > 500
            "augroup": "bot|flow-bot",
        }
        response_json = self.__try_and_retry_request(parameters)
        users = response_json['query']['allusers']
        for user in users:
            bot_set.add(user['name'])
        return bot_set

    def __try_to_get_pageviews(self, language, p, titles):
        return p.article_views(language + '.wikipedia',
                               titles,
                               agent='user',
                               granularity='monthly',
                               start=self.timestamp_start_short,
                               end=self.timestamp_end_short)

    def __get_sitelinks(self, qids):
        params = {
            'action': "wbgetentities",
            'ids': '|'.join(qids),
            'props': "sitelinks",
            'sitefilter': '|'.join(self.wikis),
            'format': "json",
        }
        sitelinks_json = self.__try_and_retry_request(params,
                                                      check_success_flag=True)
        for qid in qids:
            if "sitelinks" in sitelinks_json["entities"][qid]:
                qid_sitelinks = sitelinks_json["entities"][qid]["sitelinks"]
                for wiki in self.wikis:
                    qid_title = qid_sitelinks[wiki]["title"] if wiki in qid_sitelinks else None
                    self.df.loc[qid, wiki + "title"] = qid_title
            else:
                print("No 'sitelinks' in:", sitelinks_json["entities"][qid])
                for wiki in self.wikis:
                    self.df.loc[qid, wiki + "title"] = None

    def __get_chunks(self, l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    def __append_sitelinks_in_chunks(self):
        for wiki in self.wikis:
            self.df[wiki + "title"] = None
        qids_parts = list(self.__get_chunks(self.all_qids,
                                            self.sitelink_request_chunk_size))
        for qids_part in qids_parts:
            self.__get_sitelinks(qids_part)

    def __initialize_demand_metric(self, metric):
        self.df[metric] = 0
        for wiki in self.wikis:
            self.df[wiki + metric] = 0
        for year in self.years_l:
            self.df[metric + "_" + year] = 0
            for wiki in self.wikis:
                self.df[wiki + metric + "_" + year] = 0
    
    def __append_sitelinks_and_pageviews(self):
        self.__initialize_demand_metric("pageviews")
        self.__append_sitelinks_in_chunks()
        for lang_code in self.language_codes:
            col_lang_wikititle = lang_code + 'wikititle'
            titles = [x for x in self.df[col_lang_wikititle] if x is not None]
            titles = list(set(titles))
            if len(titles) > 0:
                time_to_retry = 12
                for attempt in range(5):
                    try:
                        pageviews = self.__try_to_get_pageviews(lang_code,
                                                                self.pvclient,
                                                                titles)
                    except:
                        time.sleep(time_to_retry)
                        time_to_retry *= 2
                    else:
                        break
                else:
                    pageviews = self.__try_to_get_pageviews(lang_code,
                                                            self.pvclient,
                                                            titles)
                for key, value in pageviews.items():
                    year = str(key)[:4]
                    col = lang_code + "wikipageviews_" + year
                    for title in titles:
                        row = self.df[col_lang_wikititle] == title
                        pageviews_value = value[title.replace(' ', '_')]
                        self.df.loc[row, col] += pageviews_value if pageviews_value else 0
        for year in self.years_l:
            self.df["pageviews_" + year] = self.df[[wiki + "pageviews_" + year for wiki in self.wikis]].sum(axis=1)
        for wiki in self.wikis:
            self.df[wiki + "pageviews"] = self.df[[wiki + "pageviews_" + year for year in self.years_l]].sum(axis=1)
        self.df["pageviews"] = self.df[[wiki + "pageviews" for wiki in self.wikis]].sum(axis=1)

    def append_contribution_metrics(self):
        self.__append_num_edits_and_activity_days()
    
    def append_demand_metrics(self):
        self.__append_sitelinks_and_pageviews()
    
    def get_df(self):
        return self.df

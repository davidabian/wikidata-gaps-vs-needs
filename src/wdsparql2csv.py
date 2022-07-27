'''
Created in 2022

@author: David Abián
'''

import re
import urllib
import requests

class WdSparql2Csv(object):
    '''
    Class to execute SPARQL queries on Wikidata, retrieve the results
    and save them as a CSV file.
    '''

    def __init__(self,
                 sparql_query,
                 offset_placeholder,
                 description,
                 chunk_size=200000,
                 url_prefix="https://query.wikidata.org/sparql?query="):
        '''
        Constructor
        '''
        sparql_regex = r'select.+?\?.+?where.+?\?.'
        print(sparql_query)
        assert re.match(sparql_regex,
                        sparql_query,
                        re.IGNORECASE|re.DOTALL)
        assert len(offset_placeholder) > 2
        assert offset_placeholder in sparql_query
        filename_regex = r'^[ A-Za-z0-9.,;:_-]{1,120}$'
        assert re.match(filename_regex, description, re.IGNORECASE)
        assert chunk_size >= 0
        url_prefix_regex = r'^https?://\S+$'
        assert re.match(url_prefix_regex, url_prefix)
        
        self.sparql_query = sparql_query
        self.offset_placeholder = offset_placeholder
        self.description = description.strip()
        self.description = re.sub("[,;:]", "", self.description)
        self.description = re.sub("\s+", " ", self.description)
        if self.description[-4:] in [".csv", ".CSV"]:
            self.description = self.description[:-4]
        self.chunk_size = chunk_size
        self.url_prefix = url_prefix
        
        with open(self.description + ".query.txt", "w") as output_file:
            output_file.write(self.sparql_query)

    def __run_sparql_query(self, query, output_filename):
        encoded_query = urllib.parse.quote(query)
        print(encoded_query)
        url = "https://query.wikidata.org/sparql?query=" + encoded_query
        headers = { "Accept": "text/csv" }
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            raise ValueError(r)
        num_qids = r.text.count("http://www.wikidata.org/entity/Q")
        output_data = r.text.replace("http://www.wikidata.org/entity/Q", "Q")
        with open(output_filename, "w") as output_file:
            output_file.write(output_data)
        return num_qids
    
    def __run_sparql_query_in_chunks(self):
        print(self.description)
        keep_iterating = True
        chunks = 0
        subquery = self.sparql_query.replace(self.offset_placeholder,
                                             "OFFSET 0 LIMIT " + str(self.chunk_size))
        while keep_iterating:
            print(subquery)
            output_filename = self.description + "." + str(chunks) + ".csv"
            num_qids = self.__run_sparql_query(subquery, output_filename)
            if num_qids < 1:
                keep_iterating = False
            else:
                chunks += 1
                subquery = self.sparql_query.replace(self.offset_placeholder,
                                         "OFFSET " + str(chunks * self.chunk_size)
                                         + " LIMIT " + str(self.chunk_size))
        # merge chunks
        input_filenames = [self.description + "." + str(chunk) + ".csv" for chunk in range(chunks)] # exclude last file, empty
        print(input_filenames)
        first_line_read = False
        output_filename = self.description + '.csv'
        with open(output_filename, 'w') as output_file:
            for input_filename in input_filenames:
                with open(input_filename) as input_file:
                    if first_line_read:
                        next(input_file)
                    else:
                        first_line_read = True
                    for line in input_file:
                        output_file.write(line)
        return output_filename

    def sparql_to_csv(self):
        return self.__run_sparql_query_in_chunks()

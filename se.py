#!/usr/bin/python
import sys
import time
from multiprocessing import Process
from datetime import datetime
from urlparse import urlparse
import random
import logging
import requests
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
import ssl
import hashlib
from urlparse import urlparse
import urllib3
from nltk.tokenize import word_tokenize
import re
import string
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk.stem.snowball import SnowballStemmer
from nltk.stem.wordnet import WordNetLemmatizer
import redis
#----- Settings & Start object created -------#
urllib3.disable_warnings()
es = Elasticsearch()
redisObj=redis.Redis()
pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
redisObj = redis.Redis(connection_pool=pool)
domain = sys.argv[1]
datetime_str = datetime.now().strftime("%Y-%m-%d %H:%M")
clean_domain = urlparse(domain)
logging.basicConfig(filename='/root/log/se_'+clean_domain.netloc+'_'+datetime_str+'.log',level=logging.DEBUG)
print_all=1
logging_all=1
logging.debug(datetime_str+' - SE started successfully')
esIndex="searchengine1"
esType="searchEngineType1"
save=1
#----- End Settings & Start object created -------#

def getDateTime():
	return datetime.now().strftime("%Y-%m-%d %H:%M")

def geturl(soup,domain,clean=0):
  arr=[]
  for link in soup.findAll("a"):
    line_href=link.get("href")
    if line_href=='#' or line_href=='/':
      continue
    url_c=line_href
    url_p=urlparse(line_href)
    if url_p.netloc != domain and url_p.netloc !='':
	  continue
    if url_p.netloc =='':
      url_c=clean_domain.netloc+'/'+line_href
    if url_p.scheme =='':
      url_c=clean_domain.scheme+'://'+url_c
    line_href=url_c
    arr.append(line_href)
  return arr

def removeEmptyLines(soup):
	text = soup.get_text()
	return "\n".join([ll.rstrip() for ll in text.splitlines() if ll.strip()])

'''From Example 1'''
def tokenized_docs(text,raw=1):
	if raw==1:
		return [word_tokenize(doc) for doc in [text]]
	else:
		return ' '.join(tokenized_docs[0]).encode('utf8')

'''From Example 2'''
def tokenized_docs_no_punctuation(tokenized_docs,raw=1):
	regex = re.compile('[%s]' % re.escape(string.punctuation)) #see documentation here: http://docs.python.org/2/library/string.html
	tokenized_docs_no_punctuation = []
	for review in tokenized_docs:
		new_review = []
		for token in review:
			new_token = regex.sub(u'', token)
			if not new_token == u'':
				new_review.append(new_token)
		tokenized_docs_no_punctuation.append(new_review)
		if raw==1:
			return tokenized_docs_no_punctuation
		else:
			return ' '.join(tokenized_docs_no_punctuation[0]).encode('utf8')

'''From Example 3'''
def tokenized_docs_no_stopwords(tokenized_docs_no_punctuation,raw=1):
	tokenized_docs_no_stopwords = []
	for doc in tokenized_docs_no_punctuation:
		new_term_vector = []
		for word in doc:
			if not word in stopwords.words('english'):
				new_term_vector.append(word)
		tokenized_docs_no_stopwords.append(new_term_vector)
	if raw==1:
		return tokenized_docs_no_stopwords
	else:
		return ' '.join(tokenized_docs_no_stopwords[0]).encode('utf8')

'''From Example 4'''
def preprocessed_docs(tokenized_docs_no_stopwords,raw=1):
	porter = PorterStemmer()
	snowball = SnowballStemmer('english')
	wordnet = WordNetLemmatizer()
	preprocessed_docs = []
	for doc in tokenized_docs_no_stopwords:
		final_doc = []
		for word in doc:
			#final_doc.append(porter.stem(word))
			#final_doc.append(snowball.stem(word))
			final_doc.append(wordnet.lemmatize(word)) #note that lemmatize() can also takes part of speech as an argument!
		#print final_doc
		if raw==1:
			return final_doc
		else:
			return ' '.join(final_doc).encode('utf8')

def ProcessKeyword(soup,raw=1):
	### Start Keywords process to search ###
	# kill all script and style elements
	for script in soup(["script", "style"]):
		script.extract()    # rip it out
	final_str = removeEmptyLines(soup)
	tokenized_docs1 = tokenized_docs(final_str)
	tokenized_docs_no_punctuation1 = tokenized_docs_no_punctuation(tokenized_docs1)
	tokenized_docs_no_stopwords1 = tokenized_docs_no_stopwords(tokenized_docs_no_punctuation1)
	preprocessed_docs1 = preprocessed_docs(tokenized_docs_no_stopwords1)
	if raw==1:
			return preprocessed_docs1
	else:
		return ' '.join(preprocessed_docs1).encode('utf8')
	### End Keywords process to search ###			
def worker(i):
	while True:
		str1=redisObj.spop(clean_domain.netloc)
		if str1 == "" or str1 == "None" or str1 == None:
			print 'found nothing returning from thread'+i
			return
		if redisObj.sismember(clean_domain.netloc+'_complete',str1) != True:
			logging.info('Thread - '+str(i)+' is processing '+str1)
			## Processing Start with 1st request
			rand1=random.randint(1, 30)
			rand2=random.randint(1, 30)
			user_agent = {'User-agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.14'+str(rand1)+'.90 Safari/537.'+str(rand2)+''}
			try:
				requestObj = requests.get(str1, headers = user_agent)
			except:
				print "request failed"+str1
				continue
			soup = BeautifulSoup(requestObj.text, "html.parser")
			try:
				link_array1 = geturl(soup,clean_domain.netloc,1)
			except:
				print "links adding failed"
			###print link_array
			ProcessKeyword1 = ProcessKeyword(soup)
			try:
				redisObj.sadd(clean_domain.netloc, *set(link_array1))#add links
				redisObj.sadd(clean_domain.netloc+"_complete",str1)
			except:
				print "sadd failed"
				print '-------start---'
				print link_array1
				print '-------end---'
				continue
			try:
				if save==1:
					uniq_md5=hashlib.md5(str1).hexdigest()
					doc = {'unix_time':int(round(time.time())),'link':str1,'text':ProcessKeyword1,'timestamp': datetime.now()}
					res = es.index(index=esIndex, doc_type=esType,id=uniq_md5, body=doc)
				else: 
					print ProcessKeyword1
			except:				
				print 'Sorry saving process failed...'+str1
	return

## Processing Start with 1st request
rand1=random.randint(1, 30)
rand2=random.randint(1, 30)
user_agent = {'User-agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.14'+str(rand1)+'.90 Safari/537.'+str(rand2)+''}
requestObj = requests.get(domain, headers = user_agent)
soup = BeautifulSoup(requestObj.text, "html.parser")
#link_array = geturl(soup)
link_array = geturl(soup,clean_domain.netloc,1)
#print link_array
ProcessKeyword1 = ProcessKeyword(soup)
redisObj.sadd(clean_domain.netloc, *set(link_array))#add links
redisObj.sadd(clean_domain.netloc+"_complete",domain)
try:
	if save==1:
		uniq_md5=hashlib.md5(domain).hexdigest()
		doc = {'unix_time':int(round(time.time())),'link':domain,'text':ProcessKeyword1,'timestamp': datetime.now()}
		res = es.index(index=esIndex, doc_type=esType,id=uniq_md5, body=doc)
	else: 
		print ProcessKeyword1
except:				
	print 'Sorry saving process failed...'

for i in range(2):
	t=Process(target=worker,args=(i,))
	t.start()
	#t.join()

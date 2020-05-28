#!/usr/bin/python3

from os import sys
import re
from collections import Counter
import pandas as pd
from matplotlib import pyplot as plt

class Enron_Mail_Solution:
	def __init__(self, file):
		# read csv file in
		self.data = pd.read_csv(file, names=['time', 'msg_id', 'sender', 'recipients', 'topic', 'mode'])
		
		# sender counts
		sender_list = []
		for sender in self.data['sender'].tolist():
			sender_str = str(sender)
			sender_str = re.sub('\.', string=re.sub(r'\s*[@\/].*|\s*at\s+\w+', string=sender_str.lower(), repl=''), repl=' ')
			sender_str = re.sub('\W', string=sender_str, repl=' ')
			sender_list.append(sender_str)
		self.sender_cnt = Counter(sender_list)

		# recipients counts
		received_list = []
		rcvd_nmlist1 = self.data['recipients'].values.tolist()
		rcvd_nmlist1 = map(lambda x:str(x).split('|'), rcvd_nmlist1)
		rcvd_nmlist2 = pd.DataFrame.from_records(rcvd_nmlist1)
		rcvd_nmlist3 = pd.concat([rcvd_nmlist2[col] for col in rcvd_nmlist2]).dropna()
		for i in rcvd_nmlist3:
			rcvd_str = i
			rcvd_str = re.sub('\.', string=re.sub(r'\s*[@\/].*|\s*at\s+\w+', string=rcvd_str.lower(), repl=''), repl=' ')
			rcvd_str = re.sub('\W', string=rcvd_str, repl=' ')
			received_list.append(rcvd_str)
		self.recipients_cnt = Counter(received_list)
		
		# data records
		self.records = pd.DataFrame([self.sender_cnt, self.recipients_cnt]).T.sort_values(by=0, ascending=False)


	def send_receive_cnt_report(self, file_name):
		# generate sender/recipients report
		self.records.columns = ['sent', 'received']
		self.records.to_csv(file_name)
		print('Writing sent/received email counts to '+file_name)

	def get_unique_cnt(dframe):
		# use set to count unique senders
		distinct_users = set()
		unique_count = []

		for index, row in dframe.iterrows():
			distinct_users.add(row['sender'])
			unique_count.append(len(distinct_users))

		return unique_count

	def draw_chart_mail_cnt_prolific_senders(self, file_name1, file_name2, ppl_num=10):
		# draw chart of emails from prolific senders
		prolifics = self.data[self.data['sender'].isin(self.records.index[:ppl_num].tolist())][['time', 'sender']]
		prolifics['count'] = 1 
		p_table = pd.pivot_table(prolifics, values='count', index='time', columns='sender').sort_index(ascending=True)
		
		for col in [c for c in p_table.columns if c !='sender']:
			p_table[col] = p_table[col].cumsum().fillna(method='ffill') 
			
		time = p_table.index
		p_table['date'] = pd.to_datetime(time, unit='ms')
		p_table.plot(x='date', y=[c for c in p_table.columns if c != 'date'])
		plt.title('Mails sent over time for prolific users')
		plt.legend(loc='best')
		plt.savefig(file_name1)
		print('Creating image file for emails sent over time for prolific users: '+file_name1)

		# draw chart of unique emails sent to prolific users
		rcvd_nmlist1 = self.data['recipients'].values.tolist()
		rcvd_nmlist1 = map(lambda x:str(x).split('|'), rcvd_nmlist1)
		rcvd_nmlist2 = pd.DataFrame.from_records(rcvd_nmlist1)
		m = rcvd_nmlist2

		for col in m.columns:
			m[col] = m[col].map(lambda x: str(x))
		temp = pd.concat([self.data, m], axis=1).drop(['recipients', 'msg_id', 'mode', 'topic'], axis=1)
		df_list = []

		for i in self.records.index[:ppl_num].tolist():
			b = temp[temp[temp.columns[2:]].isin([i]).T.any()].copy()
			b['unique_count'] = Enron_Mail_Solution.get_unique_cnt(b)
			b['recipient'] = i
			df_list.append(b[['time', 'unique_count', 'recipient']].copy())

		dframe = pd.concat(df_list, axis=0)
		p_table = pd.pivot_table(dframe, values='unique_count', index='time', columns='recipient').sort_index(ascending=True)
		p_table['date'] = pd.to_datetime(p_table.index, unit='ms')

		for column in [t for t in p_table.columns if t !='date']:
			p_table[column] = p_table[column].fillna(method='ffill')

		p_table.plot(x='date', y=[c for c in p_table.columns if c != 'date'])
		plt.title('Unique users sent mails over time to prolific users')
		plt.legend(loc='best')
		plt.savefig(file_name2)
		print('Creating image for unique senders over time to prolific users: '+file_name2)


if __name__ == '__main__':
	solution = Enron_Mail_Solution(sys.argv[1])
	#solution = Enron_Mail_Solution("enron-event-history-all.csv")
	solution.send_receive_cnt_report("sender_recipient_cnt.csv")
	solution.draw_chart_mail_cnt_prolific_senders('email_frm_prolifics.png', 'unique_to_prolifics.png')

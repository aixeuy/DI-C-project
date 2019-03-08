def appendToFile(file_name, line):
	f = open(file_name, 'a+')  # open file in append mode
	f.write(line+"\n")
	f.close()

def sentToStr(sent, sen):
	 return ' '.join([str(w) for w in sent])+sen

def process_semcor():
	print 'semcor'
	from nltk.corpus import semcor
	count=0
	word = 'bank'
	sen1 = 'depository_financial_institution.n.01'
	sen2 = 'bank.n.01'
	file_name = 'bank_semcor_labelled_tmp.txt'
	for f in semcor.fileids():
		sents = semcor.sents(f)
		tsents = semcor.tagged_sents(f,'sem')
		for i in range(len(sents)):
			sent = sents[i]
			if (word in sent):
				if(sen1 in str(tsents[i])):
					appendToFile(file_name,sentToStr(sent,'+'))
				elif(sen2 in str(tsents[i])):
					appendToFile(file_name,sentToStr(sent,'-'))
				else:
					appendToFile(file_name,sentToStr(sent,'0'))
				count = count+1
				print count

def process_brown():
	print 'brown'
	from nltk.corpus import brown
	count=0
	word = 'bank'
	sen1 = 'depository_financial_institution.n.01'
	sen2 = 'bank.n.01'
	file_name = 'bank_brwon.txt'
	for f in brown.fileids():
		sents = brown.sents(f)
		for i in range(len(sents)):
			sent = sents[i]
			if (word in sent):
				appendToFile(file_name,sentToStr(sent,'0'))
				count = count+1
				print count

def process_gutenberg():
	print 'gutenberg'
	from nltk.corpus import gutenberg
	count=0
	word = 'bank'
	sen1 = 'depository_financial_institution.n.01'
	sen2 = 'bank.n.01'
	file_name = 'bank_gutenberg_tmp.txt'
	for f in gutenberg.fileids():
		sents = gutenberg.sents(f)
		for i in range(len(sents)):
			sent = sents[i]
			if (word in sent):
				appendToFile(file_name,sentToStr(sent,'0'))
				count = count+1
				print count

def process_webtext():
	print 'webtext'
	from nltk.corpus import webtext
	count=0
	word = 'bank'
	sen1 = 'depository_financial_institution.n.01'
	sen2 = 'bank.n.01'
	file_name = 'bank_webtext_tmp.txt'
	for f in webtext.fileids():
		sents = webtext.sents(f)
		for i in range(len(sents)):
			sent = sents[i]
			if (word in sent):
				appendToFile(file_name,sentToStr(sent,'0'))
				count = count+1
				print count

def process_reuters():
	print 'reuters'
	from nltk.corpus import reuters
	count=0
	word = 'bank'
	sen1 = 'depository_financial_institution.n.01'
	sen2 = 'bank.n.01'
	file_name = 'bank_reuters_tmp.txt'
	for f in reuters.fileids():
		sents = reuters.sents(f)
		for i in range(len(sents)):
			sent = sents[i]
			if (word in sent):
				appendToFile(file_name,sentToStr(sent,'0'))
				count = count+1
				print count


process_semcor()
process_brown()
process_gutenberg()
process_webtext()
process_reuters()
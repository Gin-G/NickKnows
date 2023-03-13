from flask import redirect, url_for, render_template, request
import json
from nickknows import app

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/templates/header.html')
def header():
    return render_template('header.html')

@app.route('/templates/nfl_navbar.html')
def nfl_navbar():
    return render_template('nfl_navbar.html')

@app.route('/templates/nick_navbar.html')
def nick_navbar():
    return render_template('navbar.html')

@app.route('/arcade')
def arcade():
    return render_template('arcade.html')

@app.route('/lemans')
def lemans():
    return render_template('lemans.html')

@app.route('/guitar')
def guitar():
    return render_template('guitar.html')

@app.route('/skiing')
def skiing():
    return render_template('skiing.html')

@app.route('/job_parse', methods=['GET','POST'])
def job_parse():
    if request.method == 'GET':
        return render_template('job-parse.html')
    elif request.method == 'POST':
        description = request.form.get('job_desc')
        job_words = {}
        omit_words = ['and', 'to', 'of', 'in', 'the', 'with', 'or', 'and/or', 'i.e.', 'etc.', 'as', 'by', 'for', 'on', 'not', 'but', 'is', 'be', 'us']
        description = description.split(' ')
        for words in description:
            words = words.strip()
            words = words.replace('/', ' ')
            words = words.replace('\n', ' ')
            if len(words) == 0:
                pass
            else:
                words_list = words.split(' ')
                for word in words_list:
                    word = word.lower()
                    word = word.strip()
                    word = word.replace(',','')
                    word = word.replace("'",'')
                    word = word.replace(':','')
                    word = word.replace('.','')
                    word = word.replace(')','')
                    word = word.replace('(','')
                    if len(word) <= 1:
                        pass
                    elif word in omit_words:
                        pass
                    else:
                        if word not in job_words:
                            job_words[word] = 1
                        elif word in job_words:
                            job_words[word] += 1
        sorted_words = sorted(job_words.items(), key=lambda kv: kv[1], reverse=True)
        sorted_words = dict(sorted_words)
        return render_template('job-parse-count.html', job_count = sorted_words)

@app.route('/job_parse/resume', methods=['GET','POST'])
def job_parse_resume():
    if request.method == 'GET':
        job_count = request.args.get('job_count')
        job_count = job_count.replace("'",'"')
        job_count = job_count.replace("’","'")
        job_count = json.loads(job_count)
        return render_template('resume-compare.html', job_count = job_count)
    elif request.method == 'POST':
        resume_words = []
        job_words = []
        missing_words = []
        omit_words = ['and', 'to', 'of', 'in', 'the', 'with', 'or', 'and/or', 'i.e.', 'etc.', 'as', 'by', 'for', 'on', 'not', 'but', 'is', 'be', 'us']
        job_count = request.form.get('job_count')
        job_count = job_count.replace("':",' ')
        job_count = job_count.replace('":',' ')
        job_count = job_count.replace(", '",' ')
        job_count = job_count.replace(', "',' ')
        job_count = job_count.replace("{'",'')
        job_count = job_count.replace("}",'')
        job_count = job_count.split(' ')
        for word in job_count:
            if len(word) <= 1:
                pass
            else:
                job_words.append(word)
        resume_count = request.form.get('resume')
        resume_count = resume_count.replace('\n',' ')
        resume_count = resume_count.replace('/',' ')
        resume_count = resume_count.replace(',','')
        resume_count = resume_count.split(' ')
        for words in resume_count:
            words = words.lower()
            words = words.strip()
            if len(words) <= 1:
                pass
            elif words in omit_words:
                pass
            else:
                resume_words.append(words)
        for values in job_words:
            if values not in resume_words:
                missing_words.append(values)
        for items in missing_words:
            if items in resume_words:
                missing_words.remove(items)
        return render_template('resume-comp.html', missing_words = missing_words)
import boto3
import pymysql
from flask import Flask, render_template, request, session

app = Flask(__name__)

app.secret_key = "super key"

duser = "admin"
dname = "defaultdb"
dpassword = "multiweekdb"
dendpoint = "multiweekdb.clnopyq3sfwe.us-east-1.rds.amazonaws.com"

caccess_key = 'AKIAY43UJTCKXNGNBPYG'
csecret_key = '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'
cregion = 'us-east-1'

@app.route('/', methods=['GET','POST'])
def Register():
    if request.method == 'POST':
        Useremail = request.form['email']
        Userpassword = request.form['password']
        dconnect = pymysql.connect(host=dendpoint, user=duser, password=dpassword, database=dname)
        dcursor = dconnect.cursor()
        dquery = "INSERT INTO userdetails (email, password) VALUES (%s, %s);"
        dcursor.execute(dquery, (Useremail, Userpassword))
        dconnect.commit()
        dconnect.close()
        return render_template("login.html")
    
    return render_template("register.html")
    
@app.route('/login', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        lemail = request.form['lemail']
        lpassword = request.form['lpassword']
        dconnect = pymysql.connect(host=dendpoint, user=duser, password=dpassword, database=dname)
        dcursor = dconnect.cursor()
        dquery = "SELECT * From userdetails"
        dcursor.execute(dquery)
        temp = dcursor.fetchall()
        users = {}
        for item in temp:
            users[item[0]] = item[1]
        print(users, lemail, lpassword)
        dconnect.close()
        if lemail in users.keys():
            print(lemail)
            if users[lemail] == lpassword:
                session['loggedin_user'] = lemail
                return render_template("upload.html")
            else:
                return "Check your password"
    return render_template("login.html")

def S3_upload(file, fname, bucket):
    bucket_client = boto3.client('s3', aws_access_key_id=caccess_key, aws_secret_access_key=csecret_key, region_name=cregion)
    bucket_client.upload_fileobj(file, bucket, fname)
    url = bucket_client.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': fname}, ExpiresIn=3600)
    return url


def create_sub(Arn, endpoint, protocol):
    notification_client = boto3.client('sns',aws_access_key_id = caccess_key, aws_secret_access_key=csecret_key, region_name=cregion)
    response = notification_client.subscribe(TopicArn=Arn, Protocol=protocol, Endpoint=endpoint)
    return response['SubscriptionArn']

def publish(Arn, subject, url):
    notification_client = boto3.client('sns',aws_access_key_id = caccess_key, aws_secret_access_key=csecret_key, region_name=cregion)
    notification_client.publish(TopicArn=Arn, Subject=subject, Message=url)

def uploadbillingfile(file, lemail):
    try:
        dconnect = pymysql.connect(host=dendpoint, user=duser, password=dpassword, database=dname)
        dcursor = dconnect.cursor()
        dquery = "INSERT INTO filesandusers (file, email) VALUES (%s, %s);"
        dcursor.execute(dquery, (file, lemail))
        dconnect.commit()
        dconnect.close()
        return True
    except Exception as e:
        print(e)
        return False




@app.route('/uploadtocloud', methods = ['GET', 'POST'])
def uploadtocloud():
    if request.method == 'POST':
        file = request.files['inputfile']
        fname = file.filename
        usersemails = (request.form['emailone'], request.form['emailtwo'], request.form['emailthree'], request.form['emailfour'], request.form['emailfive'])
        url = S3_upload(file, fname, 'cbucketfinal')
        notification_client = boto3.client('sns',aws_access_key_id = caccess_key, aws_secret_access_key=csecret_key, region_name=cregion)
        topic = notification_client.create_topic(Name='cTopic')
        for user in usersemails:
            if user:
                Arn = topic['TopicArn']
                endpoint = user
                protocol = 'email'
                response = create_sub(Arn, endpoint, protocol)
                publish(Arn, "Please download file", url)
        user = session['loggedin_user']
        uploadtobilling = uploadbillingfile(fname, user)
        if uploadtobilling:
            dconnect = pymysql.connect(host=dendpoint, user=duser, password=dpassword, database=dname)
            dcursor = dconnect.cursor()
            dquery = "SELECT * FROM filesandusers;"
            dcursor.execute(dquery)
            uploadedfiles = dcursor.fetchall()
            print("files: ",uploadedfiles)
            dconnect.close()
            return render_template("billing.html", files = uploadedfiles)
        else:
            return "Error while uploading to billing."
        


    
if __name__ == "__main__":
    app.run(debug=True)
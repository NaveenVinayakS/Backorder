#from training_Validation_Insertion import train_validation
#train_validation(r'C:\Users\z030590\OneDrive - Alliance\Desktop\Personal\BackOrder_Prediction\Project\Training_Batch_Files')
#from prediction_Validation_Insertion import pred_validation
#from predictFromModel import prediction
#pred_val = pred_validation(r'C:\Users\z030590\OneDrive - Alliance\Desktop\Personal\BackOrder_Prediction\Project\Prediction_Batch_files')
#pred_val.pred_validation()
#pred = prediction(r'C:\Users\z030590\OneDrive - Alliance\Desktop\Personal\BackOrder_Prediction\Project\Prediction_Batch_files')
#path = pred.predictionFromModel()
from flask import Flask, render_template, request
from flask_cors import cross_origin
import time
import pandas as pd
import numpy as np
import webbrowser
from prediction_Validation_Insertion import pred_validation
from predictFromModel import prediction

import os
from flask import Flask, flash, request, redirect, url_for
from werkzeug.utils import secure_filename

from pathlib import Path
root_path = Path().absolute() 
root_path  = str(root_path)

ALLOWED_EXTENSIONS = {'csv'}

app = Flask(__name__)
#app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

import warnings
warnings.filterwarnings("ignore")


app = Flask(__name__) 


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            print(os.path.join(root_path, filename))
            file.save(os.path.join(root_path, filename))
            #return redirect(url_for('uploaded_file',filename=filename))

        pred = prediction('Prediction_Batch_files')
        path = pred.predictionFromModel()
        print("Processing completed and saved in folder")
        #data = pd.read_csv('Prediction_Output_File\Predictions_final.csv')
        #data = pd.read_csv('InputFile.csv')
        print("file readed")
        
        print(path)
        return render_template('view.html',tables=[path.to_html(index=False,classes='female')],
        titles = ['na', 'Backorder prediction Output', 'Male surfers'])
    return '''
    <!doctype html>
    <title>BackOrder Prediction please upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      <input type=file name=file>
      <input type=submit value=Upload>
    </form>
    '''
if __name__ == "__main__":
    app.run(host='0.0.0.0',port=8080,debug=True)
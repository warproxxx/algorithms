
from django import forms

class adminLoginForm(forms.Form):
    username = forms.CharField(max_length = 20, label ="*Your Username")
    password = forms.CharField(widget = forms.PasswordInput, label ="*Your Password")
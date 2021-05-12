#!/bin/python3

import requests
import os
import lightkv_pb2
import load_config
import tkinter as tk
import tkinter.messagebox

class LightKVClient(object):
    def __init__(self, ip, port):
        self.address = "http://" + ip + ":" + port
        self.service = "ProxyService"
        self.headers = {'content-type': 'application/proto'}    

    def select(self, key):
        url = self.address+"/"+self.service+"/select"
        request = lightkv_pb2.SelectRequest()
        response = lightkv_pb2.SelectResponse()
        request.key = key;
        res = requests.post(url, data=request.SerializeToString(), headers=self.headers)
        response.ParseFromString(res.content)
        if response.error.error_code == 0:
            return response.value, ""
        else:
            return "", response.error.error_message

    def delete(self, key):
        url = self.address+"/"+self.service+"/delete_"
        request = lightkv_pb2.DeleteRequest()
        response = lightkv_pb2.DeleteResponse()
        request.key = key;
        res = requests.post(url, data=request.SerializeToString(), headers=self.headers)
        response.ParseFromString(res.content)
        if response.error.error_code == 0:
            return ""
        else:
            return response.error.error_message

    def insert(self, key, value):
        url = self.address+"/"+self.service+"/insert"
        request = lightkv_pb2.InsertRequest()
        response = lightkv_pb2.InsertResponse()
        request.key = key;
        request.value = value;
        res = requests.post(url, data=request.SerializeToString(), headers=self.headers)
        response.ParseFromString(res.content)
        if response.error.error_code == 0:
            return ""
        else:
            return response.error.error_message

    def get_shards_state(self):
        url = self.address+"/"+self.service+"/get_shards_state"
        request = lightkv_pb2.ShardsStateRequest()
        response = lightkv_pb2.ShardsStateResponse()
        res = requests.post(url, data=request.SerializeToString(), headers=self.headers)
        response.ParseFromString(res.content)
        print(response)

client = None

def show_result(result):
    show_result_ = tk.Tk()
    show_result_.title("'LightKV Client'")
    show_result_.geometry('600x400')
    l1 = tk.Label(show_result_, text="value:", font=('Arial', 14))
    l1.place(x=170, y=50)
    l2 = tk.Label(show_result_, text=result, font=('Arial', 14))
    l2.place(x=250, y=50)
    def back():
        show_result_.destroy()
        select_win()
    b1 = tk.Button(show_result_, text='返回', font=('Arial', 14), width=10, height=1, command=back)
    b1.place(x=250, y=350)
    show_result_.mainloop()

def select_win():
    global client
    select_win_ = tk.Tk()
    select_win_.title("'LightKV Client'")
    select_win_.geometry('600x400')
    l1 = tk.Label(select_win_, text="key", font=('Arial', 14))
    l1.place(x=170, y=50)
    e1 = tk.Entry(select_win_, show=None, font=('Arial', 14)) 
    e1.place(x=225, y=50)
    def select_single():
        global client
        result, error_message = client.select(e1.get())
        if error_message != "":
            tkinter.messagebox.showerror(title='error', message=error_message) 
        else:
            select_win_.destroy()
            show_result(result)

    def back():
        select_win_.destroy()
        operator_win()
    b1 = tk.Button(select_win_, text='单点查询', font=('Arial', 14), width=10, height=1, command=select_single)
    b1.place(x=250, y=150)
    b4 = tk.Button(select_win_, text='返回', font=('Arial', 14), width=10, height=1, command=back)
    b4.place(x=250, y=200)
    select_win_.mainloop()
    

def insert_win():
    global client
    insert_win_ = tk.Tk()
    insert_win_.title("'LightKV Client'")
    insert_win_.geometry('600x400')
    l1 = tk.Label(insert_win_, text="Key", font=('Arial', 14))
    l1.place(x=190, y=50)
    e1 = tk.Entry(insert_win_, show=None, font=('Arial', 14)) 
    e1.place(x=225, y=50)
    l2 = tk.Label(insert_win_, text="Value", font=('Arial', 14))
    l2.place(x=170, y=100)
    e2 = tk.Entry(insert_win_, show=None, font=('Arial', 14))
    e2.place(x=225, y=100)
    def exec_insert():
        global client
        error_message = client.insert(e1.get(), e2.get())
        if error_message != "":
            tkinter.messagebox.showerror(title='error', message=error_message) 
        else:
            tkinter.messagebox.showinfo(title='info', message="插入成功") 
    b1 = tk.Button(insert_win_, text='插入', font=('Arial', 14), width=10, height=1, command=exec_insert) 
    b1.place(x=250, y=150)
    def back():
        insert_win_.destroy()
        operator_win()
    b2 = tk.Button(insert_win_, text='返回', font=('Arial', 14), width=10, height=1, command=back) 
    b2.place(x=250, y=200)
    insert_win_.mainloop()

def delete_win():
    global client
    delete_win_ = tk.Tk()
    delete_win_.title("'LightKV Client'")
    delete_win_.geometry('600x400')
    l1 = tk.Label(delete_win_, text="Key", font=('Arial', 14))
    l1.place(x=190, y=50)
    e1 = tk.Entry(delete_win_, show=None, font=('Arial', 14)) 
    e1.place(x=225, y=50)
    def exec_delete():
        global client
        error_message = client.delete(e1.get())
        if error_message != "":
            tkinter.messagebox.showerror(title='error', message=error_message) 
        else:
            tkinter.messagebox.showinfo(title='info', message="删除成功") 
    b1 = tk.Button(delete_win_, text='删除', font=('Arial', 14), width=10, height=1, command=exec_delete) 
    b1.place(x=250, y=100)
    def back():
        delete_win_.destroy()
        operator_win()
    b2 = tk.Button(delete_win_, text='返回', font=('Arial', 14), width=10, height=1, command=back) 
    b2.place(x=250, y=150)
    delete_win_.mainloop()

def operator_win():
    operater_window = tk.Tk()
    operater_window.title("'LightKV Client'")
    operater_window.geometry('600x400')
    def select():
        operater_window.destroy()
        select_win()

    def insert():
        operater_window.destroy()
        insert_win() 

    def delete():
        operater_window.destroy()
        delete_win() 

    b1 = tk.Button(operater_window, text='Insert', font=('Arial', 14), width=10, height=1, command=insert)
    b1.pack()
    b2 = tk.Button(operater_window, text='Select', font=('Arial', 14), width=10, height=1, command=select)
    b2.pack()
    b3 = tk.Button(operater_window, text='Delete', font=('Arial', 14), width=10, height=1, command=delete)
    b3.pack()
    operater_window.mainloop()

def connect_proxy():
    global client
    connect = tk.Tk()
    connect.title("'LightKV Client'")
    connect.geometry('600x400')
    l = tk.Label(connect, text='连接Proxy', font=('Arial', 14), width=10, height=2)
    l.pack()
    l1 = tk.Label(connect, text="IP", font=('Arial', 14))
    l1.place(x=200, y=50)
    e1 = tk.Entry(connect, show=None, font=('Arial', 14)) 
    e1.place(x=225, y=50)
    l2 = tk.Label(connect, text="PORT", font=('Arial', 14))
    l2.place(x=170, y=100)
    e2 = tk.Entry(connect, show=None, font=('Arial', 14))
    e2.place(x=225, y=100)

    def init_client():
        global client
        client = LightKVClient(e1.get(), e2.get())
        connect.destroy()
        operator_win()

    b = tk.Button(connect, text='连接', font=('Arial', 14), width=10, height=1, command=init_client) 
    b.place(x=250, y=150)
    connect.mainloop()

if __name__ == '__main__':
    connect_proxy()

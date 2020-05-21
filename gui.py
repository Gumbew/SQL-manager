from tkinter import *
from tkinter import filedialog, messagebox
import main
import os


def run_gui():
    def choose_src_file_callback():
        fn = filedialog.askopenfile()
        if fn:
            src_file_name.set(fn.name)

    def clear_data_callback():
        fn = src_file_name.get()
        if fn:
            print("CLR DATA!" + fn)
            main.remove_file_from_cluster(os.path.basename(fn))
        else:
            messagebox.showerror("Error", "Please specify file name!")

    def run_map_reduce():
        src = src_file_name.get()
        if not src:
            messagebox.showerror("Error", "Please choose source file!")
            return
        dest = dest_file_name.get() if dest_file_name.get() else src
        sql_query = sql_command.get()
        if not sql_query:
            messagebox.showerror("Error", "Please set SQL query!")
            return
        pfc = push_file_on_cluster.get()
        print("RUNNING mapreduce with these params:")
        print(f"src: {src}")
        print(f"dest: {dest}")
        print(f"sql: {sql_query}")
        print(f"pfc: {pfc}")
        main.run_tasks(sql_query, src, dest)

    root = Tk()

    width = 600
    height = 400
    master_name = "Map Reduce"
    root.title(master_name)

    ws = root.winfo_screenwidth()
    hs = root.winfo_screenheight()
    x_pad = ws // 2 - width // 2
    y_pad = hs // 2 - height // 2
    root.geometry(f"{width}x{height}+{x_pad}+{y_pad}")
    root.resizable(width=False, height=False)

    src_file_btn = Button(text="Choose source file", command=choose_src_file_callback)
    src_file_btn.grid(row=0, column=0)
    # src_file_btn.pack()

    src_file_name = StringVar()

    src_file_name_entry = Entry(textvariable=src_file_name, width=50)
    src_file_name_entry.grid(row=0, column=1)

    dest_file_name = StringVar()

    dest_file_label = Label(text="Dest file name (optional):")
    dest_file_label.grid(row=1, column=0)
    # dest_file_label.pack(side=LEFT)

    dest_file_name_entry = Entry(textvariable=dest_file_name)
    dest_file_name_entry.grid(row=1, column=1, pady=20)
    # dest_file_name.pack(side=LEFT)

    sql_entry_label = Label(text="Enter SQL command:")
    sql_entry_label.grid(row=2, column=0)

    sql_command = StringVar()

    sql_entry = Entry(textvariable=sql_command)
    sql_entry.grid(row=2, column=1, pady=20)
    # sql_entry.pack()

    push_file_on_cluster = IntVar()

    file_is_on_cluster = Checkbutton(text="Push file on cluster", variable=push_file_on_cluster)
    file_is_on_cluster.grid()

    clear_data_btn = Button(text="Clear data", command=clear_data_callback)
    clear_data_btn.grid(pady=20)

    map_reduce_btn = Button(text="Run map reduce", command=run_map_reduce)
    map_reduce_btn.grid(pady=20)

    get_file_btn = Button(text="Get file")
    get_file_btn.grid()

    root.mainloop()


run_gui()

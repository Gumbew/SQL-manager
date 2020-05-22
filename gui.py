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
        # src = src_file_name.get()
        # if not src:
        #     messagebox.showerror("Error", "Please choose source file!")
        #     return
        # dest = dest_file_name.get() if dest_file_name.get() else src
        sql_query = sql_command.get()
        if not sql_query:
            messagebox.showerror("Error", "Please set SQL query!")
            return
        # pfc = push_file_on_cluster.get()
        # print("RUNNING mapreduce with these params:")
        # print(f"src: {src}")
        # print(f"dest: {dest}")
        print(f"sql: {sql_query}")
        # print(f"pfc: {pfc}")
        main.run_tasks(sql_query)

    root = Tk()

    width = 800
    height = 600
    master_name = "Map Reduce"
    root.title(master_name)

    ws = root.winfo_screenwidth()
    hs = root.winfo_screenheight()
    x_pad = ws // 2 - width // 2
    y_pad = hs // 2 - height // 2
    root.geometry(f"{width}x{height}+{x_pad}+{y_pad}")
    root.resizable(width=False, height=False)

    src_file_btn = Button(root, text="Choose source file", command=choose_src_file_callback)
    src_file_btn.grid(row=0, column=0)
    src_file_btn.grid_remove()

    src_file_name = StringVar(root)

    src_file_name_entry = Entry(root, textvariable=src_file_name, width=50)
    src_file_name_entry.grid(row=0, column=1)
    src_file_name_entry.grid_remove()

    dest_file_name = StringVar(root)

    dest_file_label = Label(root, text="Dest file name (optional):")
    dest_file_label.grid(row=1, column=0)
    dest_file_label.grid_remove()

    dest_file_name_entry = Entry(root, textvariable=dest_file_name, width=50)
    dest_file_name_entry.grid(row=1, column=1, pady=20)
    dest_file_name_entry.grid_remove()

    sql_entry_label = Label(root, text="Enter SQL command:")
    sql_entry_label.grid(row=2, column=0)
    # sql_entry_label.grid_remove()

    sql_command = StringVar(root,
        value="SELECT B.Streams, A.Artist as musician, A.URL FROM A.csv INNER JOIN B.csv ON A.URL=B.URL;")

    # sql_entry = Entry(textvariable=sql_command, width=70)
    sql_entry = Text(root, height=5, width=70)
    sql_entry.insert(END, sql_command.get())
    sql_entry.grid(row=2, column=1, pady=20)
    # sql_entry.grid_remove()

    push_file_on_cluster = IntVar(root)

    file_is_on_cluster = Checkbutton(root, text="Push file on cluster", variable=push_file_on_cluster)
    file_is_on_cluster.grid()
    file_is_on_cluster.grid_remove()

    clear_data_btn = Button(root, text="Clear data", command=clear_data_callback)
    clear_data_btn.grid(pady=20)
    clear_data_btn.grid_remove()

    map_reduce_btn = Button(root, text="Run map reduce", command=run_map_reduce)
    map_reduce_btn.grid(column=1, pady=20)
    # map_reduce_btn.grid_remove()

    get_file_btn = Button(root, text="Get file")
    get_file_btn.grid()
    get_file_btn.grid_remove()

    root.mainloop()


if __name__ == "__main__":
    run_gui()

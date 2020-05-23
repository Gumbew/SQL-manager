from tkinter import *
from tkinter import filedialog, messagebox
import main
import os


def run_gui():
    def init_clear_data():
        def clear_data_callback():
            fn = clear_data_file_name.get()
            if fn:
                cl_all = clear_all.get()
                print("CLR DATA!" + fn)
                main.remove_file_from_cluster(os.path.basename(fn), cl_all)
                messagebox.showinfo("Done", "Done")
            else:
                messagebox.showerror("Error", "Please specify file name!")

        clear_data_label = Label(main_frame, text="Clear data")
        clear_data_label.grid()
        file_name = StringVar(main_frame, value="A.csv")
        clear_data_file_name = Entry(main_frame, textvariable=file_name)
        clear_data_file_name.grid()
        clear_all = IntVar(main_frame)
        clear_all_checkbutton = Checkbutton(main_frame, text="Clear all?", variable=clear_all)
        clear_all_checkbutton.grid()
        submit = Button(main_frame, text="Go", command=clear_data_callback)
        submit.grid()

    def init_push_file_on_cluster():
        def pfc_run():
            fn = src_file_name.get()
            main.push_file_on_cluster(fn, os.path.basename(fn))
            messagebox.showinfo("Done", "Done")

        def choose_src_file_callback():
            fn = filedialog.askopenfile()
            if fn:
                src_file_name.set(fn.name)
                src_file_name_label = Label(main_frame, text=src_file_name.get())
                src_file_name_label.grid()
                dest_file_name = StringVar(main_frame, os.path.basename(src_file_name.get()))
                dest_file_name_label = Label(main_frame, text="Dest file name:")
                dest_file_name_entry = Entry(main_frame, textvariable=dest_file_name)
                dest_file_name_label.grid()
                dest_file_name_entry.grid()
                pfc_btn = Button(main_frame, text="Push file on cluster", command=pfc_run)
                pfc_btn.grid()

        src_file_name = StringVar(main_frame)
        src_file_btn = Button(main_frame, command=choose_src_file_callback, text="Choose source file")
        src_file_btn.grid()

    def init_run_map_reduce():
        def run_map_reduce():
            sql_query = sql_entry.get("1.0", END)
            if not sql_query:
                messagebox.showerror("Error", "Please set SQL query!")
                return
            print(f"sql: {sql_query}")
            main.run_tasks(sql_query)
            messagebox.showinfo("Done", "Done")

        mr_label = Label(main_frame, text="Please enter SQL query:")
        mr_label.grid()
        default_sql_command = "SELECT B.Streams, A.Artist as musician, A.URL FROM A.csv INNER JOIN B.csv ON A.URL=B.URL;"
        sql_entry = Text(main_frame)
        sql_entry.grid()
        sql_entry.insert(END, default_sql_command)
        submit_btn = Button(main_frame, text="Run map reduce", command=run_map_reduce)
        submit_btn.grid()

    def init_get_file():
        pass

    def select_rb():
        choice = rb_var.get()
        for widget in main_frame.winfo_children():
            widget.destroy()
        if choice == "1":
            init_clear_data()
        elif choice == "2":
            init_push_file_on_cluster()
        elif choice == "3":
            init_run_map_reduce()
        elif choice == "4":
            init_get_file()

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

    rb_var = StringVar(root, "3")

    options = {
        "clear data": "1",
        "push file on cluster": "2",
        "run map reduce": 3,
        "get file": 4
    }
    col = 0
    rb_frame = Frame(root)
    rb_frame.grid()
    choose_label = Label(rb_frame, text="Please choose the option:")
    choose_label.grid(sticky=NSEW, columnspan=4)
    for key, value in options.items():
        rb = Radiobutton(rb_frame, text=key, value=value, variable=rb_var, command=select_rb)
        rb.grid(row=1, column=col, pady=10, padx=40)
        col += 1
    main_frame = Frame(root)
    main_frame.grid()
    select_rb()

    root.mainloop()


if __name__ == "__main__":
    run_gui()

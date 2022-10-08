"""
database.py -- TclRAL Database
"""
import logging
import tkinter

RPATH = "/Users/starr/SDEV/TclRAL"
logger = logging.getLogger(__name__)
db = tkinter.Tcl()
db.eval(f'::tcl::tm::path add {RPATH}')
logger.info("Created a TclRAL db")
relvars = None

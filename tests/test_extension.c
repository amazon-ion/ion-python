#include <stdio.h>
#include "Python.h"
#include "_ioncmodule.h"

static char *to_string(PyObject* pyString) {
    char* c_str = NULL;
    Py_ssize_t c_str_len;
#if PY_MAJOR_VERSION >= 3
    // TODO does this need to work for binary types?
    c_str = PyUnicode_AsUTF8AndSize(pyString, &c_str_len);
#else
    PyString_AsStringAndSize(pyString, &c_str, &c_str_len);
#endif
    return c_str;
}

static PyObject* from_string(char* str) {
    return PyUnicode_FromFormat("%s", str);
}

static PyObject* as_bytes(char* str) {
    return PyBytes_FromFormat("%s", str);
}

static int file_stream(ION_STREAM** stream, char* pathname, FILE** fd) {
    *fd = fopen(pathname, "wb");
    if (!*fd) {
        printf("\nERROR: can't open file %s\n", pathname);
        return -1;
    }
    return ion_stream_open_file_out(*fd, stream);
}

static int test_extension_write(PyObject* obj, PyObject* binary, BOOL to_file, char* pathname) {
    ION_STREAM *stream = NULL;
    FILE* fd = NULL;
    if (to_file) {
        file_stream(&stream, pathname, &fd);
    }
    else {
        ion_stream_open_stdout(&stream);
    }
    int res = _ionc_write(obj, binary, stream);
    res = ion_stream_close(stream);
    if (fd) {
        fclose(fd);
    }
    return res;
}

static int test_extension_read(char *filename) {
    hREADER reader;
    long size;
    char *buffer = NULL;
    PyObject *top_level_container = NULL;

    FILE *fstream = fopen(filename, "rb");
    if (!fstream) {
        return -1;
    }

    fseek(fstream, 0, SEEK_END);
    size = ftell(fstream);
    rewind(fstream);                // Set position indicator to the beginning
    buffer = (char *) malloc(size);
    fread(buffer, 1, size, fstream);  // copy the file into the buffer:
    fclose(fstream);

    ion_reader_open_buffer(&reader, (BYTE*)buffer, (SIZE)size, NULL); // NULL represents default reader options
    top_level_container = PyList_New(0);
    ionc_read_all(reader, top_level_container, FALSE, Py_True);
    ion_reader_close(reader);
    free(buffer);
}

int test_helloworld();
int test_write_to_memory();
int test_write_struct(BOOL to_file);
int test_write_list(BOOL to_file);
int test_write_int(BOOL to_file);
int test_read_timestamp();
int test_read_file(char *filename);

int main(int argc, char **argv)
{
    Py_Initialize();
    printf("--Starting tests--\n");
    //test_read_timestamp();
    test_helloworld();
    //test_write_int(TRUE);
    test_read_file("/Users/greggt/Desktop/ionc_int_out.ion");
    //test_write_to_memory();
    //test_write_struct(FALSE);
    //test_write_struct(TRUE);
    //test_write_list(FALSE);
    //test_write_list(TRUE);
    printf("\n--Done--\n");
    Py_Finalize();
}

int test_helloworld() {
    PyObject* result = helloworld(NULL);
    printf("%s\n", to_string(result));
    return 0;
}

int test_write_struct(BOOL to_file) {
    PyObject* binary = Py_True;
    PyObject* dict = PyDict_New();
    PyDict_SetItem(dict, from_string("abc"), from_string("def"));
    //PyObject* dict = from_string("abc");
    //Py_INCREF(dict);
    return test_extension_write(dict, binary, to_file, "/Users/greggt/Desktop/ionc_dict_out.ion");
}

int test_write_list(BOOL to_file) {
    PyObject* binary = Py_False;
    PyObject* list = PyList_New(0);
    PyObject* elem1 = from_string("abc");
    PyList_Append(list, elem1);
    PyObject* elem2 = from_string("def");
    PyList_Append(list, elem2);

    return test_extension_write(list, binary, to_file, "/Users/greggt/Desktop/ionc_list_out.ion");
}

int test_write_int(BOOL to_file) {
    PyObject* binary = Py_True;
    PyObject* intval = PyInt_FromLong(4294967295L);

    return test_extension_write(intval, binary, to_file, "/Users/greggt/Desktop/ionc_int_out.ion");
}

int test_write_to_memory() {
    PyObject* binary = Py_False;
    PyObject* obj = PyDict_New();
    PyDict_SetItem(obj, from_string("abc"), from_string("def"));
    PyDict_SetItem(obj, from_string("bytes"), as_bytes("bytes"));

    ION_STREAM *f_ion_stream = NULL;

    ion_stream_open_memory_only(&f_ion_stream);
    _ionc_write(obj, binary, f_ion_stream);
    POSITION len = ion_stream_get_position(f_ion_stream);
    ion_stream_seek(f_ion_stream, 0);
    // TODO if len > max int32, need to return two pages...
    BYTE* buf = (BYTE*)(malloc((size_t)len));  // TODO make sure when python decrefs the wrapper, this memory is freed. Or does Py_BuildValue copy? In which case this should free before returning. And if it does copy, consider using memoryview
    SIZE bytes_read;
    ion_stream_read(f_ion_stream, buf, (SIZE)len, &bytes_read);

    ion_stream_close(f_ion_stream);
    int i;
    for (i = 0; i < bytes_read; i++) {
        printf("%c", (char)buf[i]);
    }
    return 0;
}

int test_read_timestamp() {
    ionc_init_module();  // TODO can't access pure python things... so this isn't really useful.
    PyObject* arg = PyDict_New();
    char* timestamp_str = "2007-02-24T01:02:03.456+23:59";
    PyObject* timestamp = Py_BuildValue("y#", timestamp_str, strlen(timestamp_str));
    PyDict_SetItemString(arg, "data", timestamp);
    // TODO should be able to be provided as a positional OR keyword arg. Right now only keyword works.
    PyObject* result_sequence = ionc_read(NULL, PyTuple_New(0), arg);
    PyObject* result = PyList_GetItem(result_sequence, 0);

}

int test_read_file(char *filename) {
    //ionc_init_module();
    test_extension_read(filename);
}

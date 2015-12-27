#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/stat.h>

#include "sdk/tera.h"
#include "sdk/table_impl.h"
#include "sdk/client_impl.h"
#include "sdk/sdk_utils.h"

#include "proto/dumpdb.pb.h"

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "bfs.h"

DECLARE_string(flagfile);
DECLARE_string(tera_zk_addr_list);
DECLARE_string(tera_zk_root_path);

using namespace tera;

void Usage() {
    std::cout << "\n\
        dump <tablename> \n\
        restore <tablename> \n" 
        << std::endl;
}

int main(int argc, char* argv[])
{
    if (argc < 2) {
        Usage();
        return -1;
    }
    int ret = 0;
    ErrorCode error_code;
    std::string tablename = argv[2];
    Client* client = Client::NewClient(FLAGS_flagfile, NULL);
    if (strcmp(argv[1], "dump") == 0) {
        if (argc != 3) {
            Usage();
            return -1;
        }
        //dump schema first 
        TableMeta table_meta;
        TabletMetaList tablet_list;

        tera::ClientImpl* client_impl = static_cast<tera::ClientImpl*>(client);
        if (!client_impl->ShowTablesInfo(tablename, &table_meta, &tablet_list, &error_code)) {
            std::cout << "Get table meta error." << std::endl;
            return -1;
        }
        std::string schema_str = GetTableSchema(table_meta.schema(), false);
        if (table_meta.schema().kv_only()) {
            schema_str.resize(schema_str.size() - 12);
        }
        bfs::FS* fs = NULL;
        if (!bfs::FS::OpenFileSystem("Porsche:8828", &fs)) {
            std::cout << "Open filesystem fail" << std::endl;
            return -1;
        }
        if (!fs->CreateDirectory(std::string("/dump_data/" + tablename + "/").c_str())) {
            std::cout << "Create dir fail" << std::endl;
            return -1;
        }
        bfs::File* schema_file;
        if (!fs->OpenFile(std::string("/dump_data/" + tablename + "/schema").c_str(),
                    O_WRONLY | O_TRUNC, &schema_file)) {
            std::cout << "Open schema file fail" << std::endl;
            return -1;
        }
        int32_t write_bytes = schema_file->Write(schema_str.c_str(), schema_str.size());
        if (write_bytes != (int32_t)schema_str.size()) {
            std::cout << "Write schema file fail" << std::endl;
            return -1;
        }
        if (!fs->CloseFile(schema_file)) {
            std::cout << "Close schema file error" << std::endl;
            return -1;
        }
        delete schema_file;

        Table* table = NULL;
        if ((table = client->OpenTable(tablename, &error_code)) == NULL) {
            std::cout << "Open table: " << tablename << " error." << std::endl;
            return -1;
        }

        std::string start_rowkey = "";
        std::string end_rowkey = "";
        ResultStream* result_stream;
        ScanDescriptor desc(start_rowkey);
        desc.SetEnd(end_rowkey);
        desc.SetBufferSize(10240);
        desc.SetAsync(false);
        desc.SetMaxVersions(std::numeric_limits<int>::max());
        if ((result_stream = table->Scan(desc, &error_code)) == NULL) {
            std::cout << "Scan src table error" << std::endl;
            return -1;
        }
        bfs::File* data_file;
        if (!fs->OpenFile(std::string("/dump_data/" + tablename + "/data").c_str(),
                    O_WRONLY | O_TRUNC, &data_file)) {
            std::cout << "Open data file fail" << std::endl;
            return -1;
        }
        while (!result_stream->Done()) {
            /*
            std::cout << result_stream->RowName() << ":"
                << result_stream->ColumnName() << ":"
                << result_stream->Timestamp() << ":"
                << result_stream->Value() << std::endl;
            */
            tera::DumpRecord record;
            std::string columnfamily = result_stream->ColumnName();
            if (columnfamily.find(":") != std::string::npos) {
                columnfamily.resize(columnfamily.find(":"));
            }
            record.set_rowname(result_stream->RowName());
            record.set_columnfamily(columnfamily);
            record.set_qualifier(result_stream->Qualifier());
            record.set_timestamp(result_stream->Timestamp());
            record.set_value(result_stream->Value());
            std::string infobuf;
            record.SerializeToString(&infobuf);
            int32_t buf_size = infobuf.size();
            char len[4];
            for (int i = 0; i < 4; i++) {
                len[i] = (char)(buf_size & 0xff);
                buf_size >>= 8;
            }
            data_file->Write(&len[0], 4);
            data_file->Write(infobuf.data(), infobuf.size());
            result_stream->Next();
        }
        if (!fs->CloseFile(data_file)) {
            std::cout << "Close file error" << std::endl;
            return -1;
        }
        delete data_file;
        std::cout << "dump table " << tablename << " successfully" << std::endl;
        delete table;
        delete fs;
    } else if (strcmp(argv[1], "restore") == 0) {
        if (argc != 3) {
            Usage();
            return -1;
        }

        bfs::FS* fs = NULL;
        if (!bfs::FS::OpenFileSystem("Porsche:8828", &fs)) {
            std::cout << "Open filesystem fail" << std::endl;
            return -1;
        }
        bfs::File* schema_file_;
        if (!fs->OpenFile(std::string("/dump_data/" + tablename + "/schema").c_str(),
                    O_RDONLY, &schema_file_)) {
            std::cout << "Open schema file fail" << std::endl;
            return -1;
        }
        std::string  schema_info;
        char schema_buf[1024];
        while (1) {
            int len = schema_file_->Read(schema_buf, sizeof(schema_buf));
            if (len <= 0) {
                break;
            }
            schema_info += std::string(&schema_buf[0], len);
        }
        fs->CloseFile(schema_file_);
        delete schema_file_;
        schema_file_ = NULL;

        TableDescriptor table_desc;
        std::vector<std::string> delimiters;
        ParseTableSchema(schema_info, &table_desc);
        if (!client->CreateTable(table_desc, delimiters, &error_code)) {
            std::cout << "Create table error" << std::endl;
            return -1;
        }
        //restore records
        Table* table = NULL;
        if ((table = client->OpenTable(tablename, &error_code)) == NULL) {
            std::cout << "Open dest table error" << std::endl;
            return -1;
        }

        bfs::File* data_file;
        if (!fs->OpenFile(std::string("/dump_data/" + tablename + "/data").c_str(), O_RDONLY, &data_file)) {
            std::cout << "Open data file fail" << std::endl;
            return -1;
        }
        while (1) {
            long message_size = 0;
            char len_buf[4];
            int read_len = data_file->Read(len_buf, 4);
            if (read_len < 4) {
                break;
            }
            for (int i = 3; i >= 0; i--) {
                message_size = (message_size << 8) | len_buf[i];
            }
            assert(message_size > 0);
            char* message_buf = new char(message_size);
            read_len = data_file->Read(message_buf, message_size);
            assert(read_len == (int32_t)message_size);
            tera::DumpRecord record;
            record.ParseFromArray(message_buf, message_size);
            delete message_buf;
            std::cout << record.rowname() << "  " << record.value() << std::endl;
            std::string columnfamily = "";
            std::string qualifier = "";
            if (record.has_columnfamily()) {
                columnfamily = record.columnfamily();
                qualifier = record.has_qualifier() ? record.qualifier() : "";
            }
            if (!table->Put(record.rowname(), columnfamily, qualifier, record.value(), &error_code)) {
                return -1;
            }
        }
        fs->CloseFile(data_file);
        delete data_file;
        data_file = NULL;
        std::cout << "restore table " << tablename << " successfully" << std::endl;
        delete table;
        delete fs;
    } else {
        Usage();
        ret = -1;
    }
    delete client;
    return ret;
}

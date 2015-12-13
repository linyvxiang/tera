#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/stat.h>

#include "sdk/tera.h"
#include "sdk/table_impl.h"
#include "sdk/client_impl.h"
#include "sdk/sdk_utils.h"

#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "proto/dumpdb.pb.h"

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
        mkdir("./dump_data", 0777);
        mkdir(std::string("./dump_data/" + tablename).c_str(), 0777);
        std::string prefix = "./dump_data/" + tablename + "/";
        std::ofstream  schema_s(std::string(prefix + "schema").c_str());
        schema_s << schema_str;
        schema_s.close();

        Table* table = NULL;
        if ((table = client->OpenTable(tablename, &error_code)) == NULL) {
            std::cout << "Open table: " << tablename << " error." << std::endl;
            return -1;
        }
        leveldb::DB* db;
        leveldb::Options options;
        options.create_if_missing = true;
        leveldb::Status s = leveldb::DB::Open(options, prefix, &db);
        if (!s.ok()) {
            std::cout << "Open dest db error" << std::endl;
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
            record.set_columnfamily(columnfamily);
            record.set_qualifier(result_stream->Qualifier());
            record.set_timestamp(result_stream->Timestamp());
            record.set_value(result_stream->Value());
            std::string infobuf;
            record.SerializeToString(&infobuf);
            leveldb::Status s = db->Put(leveldb::WriteOptions(), result_stream->RowName(), infobuf);
            assert(s.ok());
            result_stream->Next();
        }
        std::cout << "dump table " << tablename << " successfully" << std::endl;
        delete table;
        delete db;
    } else if (strcmp(argv[1], "restore") == 0) {
        if (argc != 3) {
            Usage();
            return -1;
        }
        std::string prefix = "./dump_data/" + tablename;
        std::string schema_file = prefix + "/schema";
        TableDescriptor table_desc;
        if (!ParseTableSchemaFile(schema_file, &table_desc)) {
            std::cout << "Parse schema error" << std::endl;
            return -1;
        }
        std::vector<std::string> delimiters;
        /*
        std::string delimiter_file = prefix + "/delimitr";
        if (!ParseDelimiterFile(delimiter_file, &delimiters)) {
            std::cout << "Parse delimiter_file error" << std::endl;
            return -1;
        }
        */
        if (!client->CreateTable(table_desc, delimiters, &error_code)) {
            std::cout << "Create table error" << std::endl;
            return -1;
        }

        //restore records
        leveldb::DB* db;
        leveldb::Options options;
        options.create_if_missing = false;
        leveldb::Status s = leveldb::DB::Open(options, prefix, &db);
        assert(s.ok());

        Table* table = NULL;
        if ((table = client->OpenTable(tablename, &error_code)) == NULL) {
            std::cout << "Open dest table error" << std::endl;
            return -1;
        }

        leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            tera::DumpRecord record;
            record.ParseFromArray(it->value().data(), it->value().size());
            std::string row_key(it->key().data(), it->key().size());
            std::string columnfamily = "";
            std::string qualifier = "";
            if (record.has_columnfamily()) {
                columnfamily = record.columnfamily();
                qualifier = record.has_qualifier() ? record.qualifier() : "";
            }
            if (!table->Put(row_key, columnfamily, qualifier, record.value(), &error_code)) {
                return -1;
            }
        }
        std::cout << "restore table " << tablename << " successfully" << std::endl;
        delete it;
        delete table;
        delete db;
    } else {
        Usage();
        ret = -1;
    }
    delete client;
    return ret;
}

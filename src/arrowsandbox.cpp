//============================================================================
// Name        : .cpp
// Author      : 
// Version     :
// Copyright   : Your copyright notice
// Description :
//============================================================================

#include <string>
#include <unordered_map>
#include <map>
#include <random>
#include <iterator>
#include <functional>
#include <sys/time.h>
#include <stddef.h>
#include <vector>
#include <assert.h>
#include <algorithm>
#include <iostream>
#include <chrono>

#include <arrow/api.h>
#include <arrow/ipc/feather.h>
//#include <arrow/python/api.h>

#include <arrow/io/file.h>

#include <python2.7/Python.h>

typedef unsigned long long timestamp_t;

static timestamp_t get_timestamp()
{
  struct timeval now;
  gettimeofday (&now, NULL);
  return  now.tv_usec + (timestamp_t)now.tv_sec * 1000000;
}

using namespace std;

typedef multimap<string, int> StringToIntMap;
typedef StringToIntMap::iterator mapIter;

template<typename Diff>
void log_progress(Diff d)
{
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(d).count()
              << " ms passed" << std::endl;
}
template<typename Diff>
void log_rate(Diff d, double table_size)
{
    std::cout <<"Rate:" <<(table_size/((double)std::chrono::duration_cast<std::chrono::seconds>(d).count()))
              << " MB/s" << std::endl;
}
using namespace arrow;


int main() {
    //std::cout.sync_with_stdio(false); // on some platforms, stdout flushes on \n
    std::cout << "Begin arrow sandbox\n";

    struct data_row {
        int64_t id;
        double cost;
        std::vector<double> cost_components;
     };
    data_row foo1;
    foo1.id = 0;
    foo1.cost = 2.2;
    foo1.cost_components.push_back(0.34242);
    std::cout<<"Packing vector with artificial data." <<std::endl;
    std::vector<data_row> rows;
    //size_t num_rows = 10;
    //size_t num_vec = 100000000;
    size_t num_rows = 10;
    size_t num_vec  = 1000000;
    double allocation_size = (num_rows * 2 + num_rows*num_vec)*8/(1.0e6);
    std::cout<<"size of data allocation: " << allocation_size << " MB" <<std::endl;
    rows.reserve((num_rows * 3));
    auto t1i = std::chrono::high_resolution_clock::now();
    for(size_t yy = 0; yy<num_vec; yy++){
     foo1.cost_components.push_back(1.2 * (double)yy + 1);
    }
    for(size_t xx=0; xx<num_rows;xx++)
    {
        foo1.id = xx;
        rows.push_back(foo1);
    }
    auto nowi = std::chrono::high_resolution_clock::now();
    log_progress(nowi - t1i);
    std::cout<<"Building an arrow array." <<std::endl;

    auto t1b = std::chrono::high_resolution_clock::now();
    MemoryPool *pool = arrow::default_memory_pool();
    arrow::Int64Builder  id_builder(pool);
    arrow::DoubleBuilder cost_builder(pool);

   std::shared_ptr<DataType> value_type = float64();
   std::shared_ptr<DataType> type       = list(value_type);
   std::shared_ptr<ListBuilder>  components_builder;
   std::unique_ptr<ArrayBuilder> tmp;

   MakeBuilder(pool, type, &tmp);
   components_builder.reset(static_cast<ListBuilder*>(tmp.release()));
   //components_builder.reset(static_cast<ListBuilder*>(tmp));
   components_builder->Reserve(num_rows);

   //std::shared_ptr<DoubleBuilder> components_values_builder = std::make_shared<DoubleBuilder>(pool, components_builder.value_builder());
   DoubleBuilder *components_values_builder = static_cast<DoubleBuilder*>(components_builder->value_builder());
   //ArrayBuilder *components_values_builder = static_cast<ArrayBuilder*>(components_builder->value_builder());
   //components_values_builder->
   components_values_builder->Reserve(num_vec);
   //DoubleBuilder* char_vb = static_cast<DoubleBuilder*>(list_vb->value_builder());
   auto nowb = std::chrono::high_resolution_clock::now();
   log_progress(nowb - t1b);
   auto t1 = std::chrono::high_resolution_clock::now();
   for (const data_row& row : rows) {
         id_builder.Append(row.id);
         cost_builder.Append(row.cost);
         // Indicate the start of a new list row. This will memorize the current
         // offset in the values builder.
         components_builder->Append();
         // Store the actual values. The final nullptr argument tells the underlying
         // builder that all added values are valid, i.e. non-null.
         //ARROW_RETURN_NOT_OK(components_values_builder->Append(row.cost_components.data(), row.cost_components.size(), nullptr));
         cout<< "row size:" << row.cost_components.size() << " data:" << row.cost_components.data()[0] << " data:" << row.cost_components.data()[1]<< std::endl;
         //components_values_builder->AppendNulls((const unsigned char *)row.cost_components.data(),(int64_t) row.cost_components.size());
         //components_builder->value_builder()->Append(row.cost_components.data(),(int64_t) row.cost_components.size());
         components_values_builder->Append(row.cost_components.data(), row.cost_components.size());
   }
    std::cout<<"FOO0 values:"<< components_values_builder->length() << ":" << components_values_builder->null_count() << std::endl;

    //Finalize the array we just created
    std::shared_ptr<arrow::Array> id_array;
    id_builder.Finish(&id_array);
    std::shared_ptr<arrow::Array> cost_array;
    cost_builder.Finish(&cost_array);
    std::shared_ptr<arrow::Array> cost_components_array;
    components_builder->Finish(&cost_components_array);
    //cost_components_array->data()->
    std::cout<<"FOO1 values:"<< components_builder->capacity() << ":" << components_builder->length() <<":" << components_builder->length()<< ":" << components_builder->null_count() <<std::endl;
    std::cout<<"FOO2 values:"<< cost_components_array->data()->null_count << ":" <<cost_components_array->data()->length <<":"<<cost_components_array->length() << ":" << cost_components_array->num_fields()<< ":" << cost_components_array->null_count() <<std::endl;

    auto now = std::chrono::high_resolution_clock::now();
    log_progress(now - t1);
    log_rate(now - t1, allocation_size);
    //
   std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
   arrow::field("id", arrow::int64()),
   arrow::field("cost", arrow::float64()),
   arrow::field("cost_components", arrow::list(arrow::float64()))
   };
   auto schema = std::make_shared<arrow::Schema>(schema_vector);
   std::shared_ptr<arrow::Table> table;

   MakeTable(schema, {id_array, cost_array, cost_components_array}, &table);
   auto expected_schema = std::make_shared<arrow::Schema>(schema_vector);

   if (!expected_schema->Equals(*table->schema())) {
        // The table doesn't have the expected schema thus we cannot directly
        // convert it to our target representation.
        // TODO: Implement your custom error handling logic here.
   }else{
       std::cout<<"Schema was successfully matched." <<std::endl;
   }
///////////////////////////////
   // For simplicity, we assume that all arrays consist of a single chunk here.
   // In a productive implementation this should either be explicitly check or code
   // added that can treat chunked arrays.
   auto ids      = std::static_pointer_cast<arrow::Int64Array>(table->column(0)->data()->chunk(0));
   auto costs    = std::static_pointer_cast<arrow::DoubleArray>(table->column(1)->data()->chunk(0));
   auto cost_components        = std::static_pointer_cast<arrow::ListArray>(table->column(2)->data()->chunk(0));
   auto cost_components_values = std::static_pointer_cast<arrow::DoubleArray>(cost_components->values());

   // To enable zero-copy slices, the native values pointer might need to account
   // for this slicing offset. This is not needed for the higher level functions
   // like Value(…) that already account for this offset internally.
   const double* cost_components_values_ptr = cost_components_values->raw_values();
   //const double* cost_components_values_ptr = cost_components_values->values()->data();
   //const double* cost_components_values_ptr = cost_components_values->raw_values();
   std::cout<<"cost_components_values-offset:" << cost_components_values->offset() << std::endl;

   std::vector<data_row> rowsout;
   for (int64_t i = 0; i < table->num_rows(); i++) {
       // Another simplification in this example is that we assume that there are
       // no null entries, e.g. each row is fill with valid values.
       int64_t id  = ids->Value(i);
       double cost = costs->Value(i);
       std::cout<<"id:" << id << " cost:" << cost << std::endl;
       std::cout<<"value_offset:" << cost_components->value_offset(i) << " value_offset1:" << cost_components->value_offset(i + 1) << std::endl;
       std::cout<<"cost_components->raw_value_offsets[0]:" <<  cost_components->raw_value_offsets()[0] << " cost_components->raw_value_offsets[1]:" << cost_components->raw_value_offsets()[1] << std::endl;

       const double* first   = cost_components_values_ptr + cost_components->raw_value_offsets()[i];
       //const double* first   = cost_components_values_ptr;
       const double* last    = cost_components_values_ptr + cost_components->raw_value_offsets()[i+1];
       std::vector<double> components_vec(first, last);
       int foo = 0;
       for(auto const& a: components_vec) {
       std::cout<< "vector: " << a <<std::endl;
       foo++;
       if(foo>5)
           break;
       }
       break;
           //rowsout.push_back({id, cost, components_vec});
   }

   //////////////////////////////
   // Cast the Array to its actual type to access its data
   std::shared_ptr<Int64Array> int64_array = std::static_pointer_cast<Int64Array>(cost_components_array);

   // Get the pointer to the null bitmap.
   const uint8_t* null_bitmap = int64_array->null_bitmap_data();

   // Get the pointer to the actual data
   const int64_t* data      = int64_array->raw_values();
   const int64_t size_array = int64_array->length();
   const int64_t size_null  = int64_array->null_count();

   std::cout<<"Num fields from finalized array:" << int64_array->num_fields() << std::endl;
   // Let's learn how to write out the feather data and read it back in
   // to an R data frame.
   auto tf = std::chrono::high_resolution_clock::now();
   std::cout<<"Starting feather write. size_array:" << size_array <<" size null:" << size_null <<std::endl;

   std::string path="/tmp/v1.feather";
   std::string str_path(path);
   bool append = true;

   //writer->OpenFile(str_path, &writer);
   /*static Status Open(const std::shared_ptr<io::OutputStream>& stream,
                        std::unique_ptr<TableWriter>* out); */
   std::shared_ptr<arrow::io::FileOutputStream> file;
   arrow::io::FileOutputStream::Open(path, append, &file);
   file->Write(reinterpret_cast<const uint8_t*>(data), size_array);
   int fd = file->file_descriptor();
   file->Close();
   std::cout<<"Feather write finish." <<std::endl;
   auto nowf = std::chrono::high_resolution_clock::now();
   log_progress(nowf - tf);

   //Table writer
   //std::shared_ptr<io::BufferOutputStream> stream;
   //std::unique_ptr<arrow::ipc::feather::TableReader> reader;
   /*std::shared_ptr<arrow::io::BufferOutputStream> stream;
   std::unique_ptr<arrow::ipc::feather::TableWriter> writer;
   std::unique_ptr<arrow::ipc::feather::TableReader> reader;
   std::shared_ptr<Buffer> output;
   arrow::io::BufferOutputStream::Create(1024, default_memory_pool(), &stream);
   arrow::ipc::feather::TableWriter::Open(stream, &writer);
*/
   //Convert to Pandas format
   //Status ConvertArrayToPandas(PandasOptions options, const std::shared_ptr<Array>& arr,
   //PyObject* py_ref, PyObject** out);
   /*
   arrow::py::PandasOptions options;
   PyObject* py_ref;
   PyObject** out;
   const std::shared_ptr<arrow::Array> & foo;
   //arrow::py::ConvertArrayToPandas(options,  foo, py_ref, out);
   PyObject* out;
   MemoryPool* poolpandas = default_memory_pool();
   arrow::py::ConvertTableToPandas(options, table, 2, poolpandas, &out);
   */


   //https://docs.python.org/2/c-api/file.html
   //int PyFile_WriteObject(PyObject *obj, PyObject *p, int flags)¶
   //int PyFile_WriteObject(PyObject *obj, PyObject *p, int flags)¶
   /*
   table->FromRecordBatches();
   RecordBatch batch;
   for (int i = 0; i < batch.num_columns(); ++i) {
        writer->Append(batch.column_name(i), *batch.column(i));
   }
   */

/*
      std::shared_ptr<Column> col;
      for (int i = 0; i < batch.num_columns(); ++i) {
        reader_->GetColumn(i, &col);
        ASSERT_EQ(batch.column_name(i), col->name());
        CheckArrays(*batch.column(i), *col->data()->chunk(0));
      }
*/
   return 0;
}


/*
arrow::Int64Builder id_builder(arrow::default_memory_pool());
arrow::DoubleBuilder cost_builder(arrow::default_memory_pool());
std::shared_ptr<DoubleBuilder> components_values_builder = std::make_shared<DoubleBuilder>(arrow::default_memory_pool());
MemoryPool* pool = default_memory_pool();

auto data = std::make_shared<PoolBuffer>(pool);
auto null_bitmap = std::make_shared<PoolBuffer>(pool);
std::unique_ptr<Int32Array> arr(new Int32Array(100, data, null_bitmap, 10));
*/


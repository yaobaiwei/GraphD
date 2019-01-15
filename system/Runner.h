#ifndef RUNNER_H
#define RUNNER_H

#include "global.h"
#include "Sender.h"
#include "Recver.h"
#include <string>
#include <thread>
using namespace std;

template <class VertexT, class ComperT>
class Runner
{
public:

	//should be called right after params is set, and !!! right before calling any run() !!!
	void prepare(const WorkerParams& params)
	{
		IOPREGEL_SENDER_DIR = params.local_root + "/sender";
		_mkdir(IOPREGEL_SENDER_DIR.c_str());
		IOPREGEL_RECVER_DIR = params.local_root + "/recver";
		_mkdir(IOPREGEL_RECVER_DIR.c_str());
		IOPREGEL_GRAPH_DIR = params.local_root + "/graph";
		_mkdir(IOPREGEL_GRAPH_DIR.c_str());
		global_hdfs_load = params.hdfs_load;
	}

	void runHH(const string hdfs_inpath, const string hdfs_outpath, const string local_root, bool dump_with_edges, int argc, char* argv[]) //HDFS Load, HDFS Dump
	{
		WorkerParams params;
		params.local_root = local_root;
		params.input_path = hdfs_inpath;
		params.output_path = hdfs_outpath;
		params.dump_with_edges = dump_with_edges;
		//---
		prepare(params);
		init_workers(argc, argv);
		//---
		ComperT c; //must be defined before Recver, as it will set global_vertices
		//it will set global_aggregator_used
		Sender<VertexT> s; //it will set global_combiner_used
		Recver<VertexT> r; //must be defined after Comper, as it will need global_vertices
		//---
		thread t1(&Sender<VertexT>::run, &s);
		thread t2(&ComperT::run, &c, ref(params));
		r.run();
		//---
		t1.join();
		t2.join();
		//---
		worker_finalize();
	}

	void runLH(const string hdfs_outpath, const string local_root, bool dump_with_edges, int argc, char* argv[]) //Local Load (read from previous local_root/graph), HDFS Dump
	{
		WorkerParams params;
		params.local_root = local_root;
		params.hdfs_load = false;
		params.output_path = hdfs_outpath;
		params.dump_with_edges = dump_with_edges;
		//---
		prepare(params);
		init_workers(argc, argv);
		//---
		ComperT c; //must be defined before Recver, as it will set global_vertices
		//it will set global_aggregator_used
		Sender<VertexT> s; //it will set global_combiner_used
		Recver<VertexT> r; //must be defined after Comper, as it will need global_vertices
		//---
		thread t1(&Sender<VertexT>::run, &s);
		thread t2(&ComperT::run, &c, ref(params));
		r.run();
		//---
		t1.join();
		t2.join();
		//---
		worker_finalize();
	}

	void runHL(const string hdfs_inpath, const string local_outpath, const string local_root, bool dump_with_edges, int argc, char* argv[]) //HDFS Load, Local Dump
	{
		WorkerParams params;
		params.local_root = local_root;
		params.hdfs_dump = false;
		params.input_path = hdfs_inpath;
		params.output_path = local_outpath;
		params.dump_with_edges = dump_with_edges;
		//---
		prepare(params);
		init_workers(argc, argv);
		//---
		ComperT c; //must be defined before Recver, as it will set global_vertices
		//it will set global_aggregator_used
		Sender<VertexT> s; //it will set global_combiner_used
		Recver<VertexT> r; //must be defined after Comper, as it will need global_vertices
		//---
		thread t1(&Sender<VertexT>::run, &s);
		thread t2(&ComperT::run, &c, ref(params));
		r.run();
		//---
		t1.join();
		t2.join();
		//---
		worker_finalize();
	}

	void runLL(const string local_outpath, const string local_root, bool dump_with_edges, int argc, char* argv[]) //Local Load (read from previous local_root/graph), Local Dump
	{
		WorkerParams params;
		params.local_root = local_root;
		params.hdfs_load = false;
		params.hdfs_dump = false;
		params.output_path = local_outpath;
		params.dump_with_edges = dump_with_edges;
		//---
		prepare(params);
		init_workers(argc, argv);
		//---
		ComperT c; //must be defined before Recver, as it will set global_vertices
		//it will set global_aggregator_used
		Sender<VertexT> s; //it will set global_combiner_used
		Recver<VertexT> r; //must be defined after Comper, as it will need global_vertices
		//---
		thread t1(&Sender<VertexT>::run, &s);
		thread t2(&ComperT::run, &c, ref(params));
		r.run();
		//---
		t1.join();
		t2.join();
		//---
		worker_finalize();
	}

	void run_recode(const string local_root, int argc, char* argv[]) //Local Load (read from previous local_root/graph), Local Dump
	{//from HDFS
		WorkerParams params;
		params.local_root = local_root;
		params.hdfs_load = false; //read from local
		params.dump_disabled = true; //no need to dump anything, write in last superstep already
		//---
		prepare(params);
		init_workers(argc, argv);
		//---
		ComperT c; //must be defined before Recver, as it will set global_vertices
		//it will set global_aggregator_used
		Sender<VertexT> s; //it will set global_combiner_used
		Recver<VertexT> r; //must be defined after Comper, as it will need global_vertices
		//---
		thread t1(&Sender<VertexT>::run, &s);
		thread t2(&ComperT::run, &c, ref(params));
		r.run();
		//---
		t1.join();
		t2.join();
		//---
		worker_finalize();
	}

	void run_recode(const string hdfs_inpath, const string local_root, int argc, char* argv[]) //Local Load (read from previous local_root/graph), Local Dump
	{//from HDFS
		WorkerParams params;
		params.local_root = local_root;
		params.dump_disabled = true; //no need to dump anything, write in last superstep already
		params.input_path = hdfs_inpath;
		//---
		prepare(params);
		init_workers(argc, argv);
		//---
		ComperT c; //must be defined before Recver, as it will set global_vertices
		//it will set global_aggregator_used
		Sender<VertexT> s; //it will set global_combiner_used
		Recver<VertexT> r; //must be defined after Comper, as it will need global_vertices
		//---
		thread t1(&Sender<VertexT>::run, &s);
		thread t2(&ComperT::run, &c, ref(params));
		r.run();
		//---
		t1.join();
		t2.join();
		//---
		worker_finalize();
	}
};

#endif

#include "Vertex.h"
#include "Comper.h"
#include "Runner.h"
#include <float.h>

//requirement: the original graph should have all vertices being active
//later, the actual job should set the correct active-state at the beginning

//recoded E-stream
ofbinstream eout;
//we do not need V-stream, it's the same
//- old ID is there
//- new ID is consecutive, and no need to store

class IDRecVertex:public Vertex<VertexID, double, VertexID, VertexID> //MsgT == KeyT
{
	public:
		virtual void compute(vector<VertexID>& msgs, vector<VertexID>& edges)
		{
			if(step_num()==1)
			{
				//send requests to out-nbs
				for(int i=0; i<edges.size(); i++) send_message(edges[i], id);
			}
			else if(step_num()==2)
			{
				//respond
				for(int i=0; i<msgs.size(); i++) send_message(msgs[i], recoded_id());
			}
			else
			{
				//append to eout
				for(int i=0; i<msgs.size(); i++) eout << 1.0 << msgs[i];
				vote_to_halt();
			}
		}
};

class IDRecComper:public Comper<IDRecVertex>
{
	char buf[100];

	public:
		virtual VertexID parseVertex(char* line, obinstream& file_stream)
		{
			char * pch = strtok(line, "\t");
			VertexID id = atoll(pch);
			file_stream << id; //write <I>
			file_stream << DBL_MAX; //write <V>, init Pr = 1.0 (cannot use 1 as it is not double)
			file_stream << true; //write <active>
			pch=strtok(NULL, " ");
			int num=atoi(pch);
			file_stream << num; //write numNbs
			for(int i=0; i<num; i++)
			{
				pch=strtok(NULL, " ");
				VertexID nb = atoll(pch);
				file_stream << nb; //write <E>
			}
			return id;
		}

		//to_line(.) will not be called, as we disable dump here

		virtual void step_begin()
		{
			if(step_num() == 3)
			{
				char tmp[100];
				char tmp2[20];
				//must set fname to be local_root/graph/E_recoded_(_my_rank), as RECRunner will read E-stream from there
				strcpy(tmp, IOPREGEL_GRAPH_DIR.c_str());
				strcat(tmp, "/E_recoded");
				sprintf(tmp2, "_%d", _my_rank);
				strcat(tmp, tmp2);
				eout.open(tmp);
			}
		}

		virtual void step_end()
		{
			if(step_num() == 3) eout.close();
		}
};

int main(int argc, char* argv[])
{
	Runner<IDRecVertex, IDRecComper> runner;
	string local_root = "iopregel_localspace";
	//runner.run_recode(local_root, argc, argv); //local Load
	string hdfs_inpath = argv[1];
	string hdfs_outpath = argv[2];
	runner.run_recode(hdfs_inpath, local_root, argc, argv); //HDFS Load
    return 0;
}
#include "Vertex.h"
#include "Comper.h"
#include "Runner.h"
#include "Combiner.h"

class CCCombiner:public Combiner<VertexID>
{
	public:
		virtual void combine(VertexID old, const VertexID& new_msg)
		{
			if (old > new_msg) old = new_msg;
		}
};

//class CCVertex:public Vertex<VertexID, VertexID, VertexID, VertexID> //no combiner
class CCVertex:public Vertex<VertexID, VertexID, VertexID, VertexID, CCCombiner> //with combiner
{
	public:
        void broadcast(vector<VertexID>& edges)
        {
            for (int i = 0; i < edges.size(); ++ i) send_message(edges[i], value);
        }
		virtual void compute(vector<VertexID>& msgs, vector<VertexID>& edges)
		{
			if(step_num()==1)
			{
				VertexID min = id;
				for (int i = 0; i < edges.size(); ++ i) 
				{
					if (min > edges[i]) min = edges[i];
				}
				value = min;
				broadcast(edges);
				vote_to_halt();
			}
			else
			{
				VertexID min = msgs[0];
				for (int i = 0; i < msgs.size(); ++ i)
				{
                    if (min > msgs[i]) min = msgs[i];
				}
                if (min < value)
                {
                    value = min;
                    broadcast(edges);
                }
			    vote_to_halt();
			}
		}
};
class CCComper:public Comper<CCVertex>
{
	char buf[100];

	public:
		virtual VertexID parseVertex(char* line, obinstream& file_stream)
		{
			char * pch = strtok(line, "\t");
			VertexID id = atoll(pch);
			file_stream << id; //write <I>
			file_stream << id; //write <V>, init color = id
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

		virtual void to_line(CCVertex& v, ofstream& fout)
		{
			fout<<v.id<<'\t'<<v.value<<endl; //report: vid \t color
		}

		virtual void to_line(CCVertex& v, BufferedWriter& fout)
		{
			sprintf(buf, "%lld\t%lld\n", v.id, v.value);
			fout.write(buf);
		}
};

int main(int argc, char* argv[])
{
	Runner<CCVertex, CCComper> runner;
	string hdfs_inpath = argv[1];
	string hdfs_outpath = argv[2];
	string local_root = "iopregel_localspace";
	bool dump_with_edges = false;
	runner.runHH(hdfs_inpath, hdfs_outpath, local_root, dump_with_edges, argc, argv); //HDFS Load, HDFS Dump
    return 0;
}

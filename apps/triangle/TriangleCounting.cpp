#include "Vertex.h"
#include "Comper.h"
#include "Runner.h"
#include "Combiner.h"

class TCVertex:public Vertex<VertexID, int, VertexID, VertexID> 
{
	public:
		virtual void compute(vector<VertexID>& msgs, vector<VertexID>& edges)
		{
			if(step_num()==1)
			{
                for (int i = 0; i < edges.size(); ++ i)
                {
                    for (int j = i+1; j < edges.size(); ++ j)
                    {
                        send_message(edges[i], edges[j]);
                    }
                }
			}
			else
			{
                sort(edges.begin(), edges.end());
				for (int i = 0; i < msgs.size(); ++ i)
				{
                    bool found = binary_search(edges.begin(), edges.end(), msgs[i]);
                    if (found) value++;
				}
			}
			vote_to_halt();
		}
};
class TCComper:public Comper<TCVertex>
{
	char buf[100];

	public:
		virtual VertexID parseVertex(char* line, obinstream& file_stream)
		{
			char * pch = strtok(line, "\t");
			VertexID id = atoll(pch);
			file_stream << id; //write <I>
			file_stream << 0; //write <V>, init count = 0
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
		virtual void to_line(TCVertex& v, BufferedWriter& fout)
		{
			sprintf(buf, "%lld\t%lld\n", v.id, v.value);
			fout.write(buf);
		}
};

int main(int argc, char* argv[])
{
	Runner<TCVertex, TCComper> runner;
	string hdfs_inpath = argv[1];
	string hdfs_outpath = argv[2];
	string local_root = "iopregel_localspace";
	bool dump_with_edges = false;
	runner.runHH(hdfs_inpath, hdfs_outpath, local_root, dump_with_edges, argc, argv); //HDFS Load, HDFS Dump
    return 0;
}

#include "Vertex.h"
#include "Comper.h"
#include "Runner.h"
#include "Combiner.h"

#define PAGERANK_ROUND 10

class PRCombiner:public Combiner<double>
{
	public:
		virtual void combine(double& old, const double& new_msg)
		{
			old += new_msg;
		}
};

//class PRVertex:public Vertex<VertexID, double, VertexID, double> //no combiner
class PRVertex:public Vertex<VertexID, double, VertexID, double, PRCombiner> //with combiner
{
	public:
		virtual void compute(vector<double>& msgs, vector<VertexID>& edges)
		{
			if(step_num()==1)
			{
				value = 1.0;
			}
			else
			{
				double sum = 0;
				for(int i=0; i<msgs.size(); i++) sum += msgs[i];
				value = 0.15 + 0.85*sum;
			}
			if(step_num() < PAGERANK_ROUND)
			{
				double msg = value / degree; //value is double, so division gives double
				for(int i=0; i<edges.size(); i++) send_message(edges[i], msg);
			}
			else vote_to_halt();
		}
};



class PRComper:public Comper<PRVertex>
{
	char buf[100];

	public:
		virtual VertexID parseVertex(char* line, obinstream& file_stream)
		{
			char * pch = strtok(line, "\t");
			VertexID id = atoll(pch);
			file_stream << id; //write <I>
			file_stream << 1.0; //write <V>, init Pr = 1.0 (cannot use 1 as it is not double)
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

		virtual void to_line(PRVertex& v, ofstream& fout)
		{
			fout<<v.id<<'\t'<<v.value<<endl; //report: vid \t pagerank
		}

		virtual void to_line(PRVertex& v, BufferedWriter& fout)
		{
			sprintf(buf, "%lld\t%lf\n", v.id, v.value);
			fout.write(buf);
		}
};

int main(int argc, char* argv[])
{
	Runner<PRVertex, PRComper> runner;
	string hdfs_inpath = argv[1];
	string hdfs_outpath = argv[2];
	string local_root = "iopregel_localspace";
	bool dump_with_edges = false;
	runner.runHH(hdfs_inpath, hdfs_outpath, local_root, dump_with_edges, argc, argv); //HDFS Load, HDFS Dump
    return 0;
}
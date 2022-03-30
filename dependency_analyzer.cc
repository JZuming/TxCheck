#include <dependency_analyzer.hh>

dependency_analyzer::dependency_analyzer(vector<one_output>& total_output,
                        vector<int>& final_tid_queue,
                        vector<stmt_usage>& final_stmt_usage,
                        int t_num):
tid_num(t_num)
{   
    dependency_graph = new dependency_type* [tid_num];
    for (int i = 0; i < tid_num; i++) 
        dependency_graph[i] = new dependency_type [tid_num];
    
    auto stmt_num = final_tid_queue.size();
    for (int i = 0; i < stmt_num; i++) {
        auto& each_output = total_output[i];
        auto tid = final_tid_queue[i];
        auto stmt_u = final_stmt_usage[i];

        // do not analyze empty output select read;
        // write operation (insert, delete, update) will be analzye by using before/after-write read
        if (each_output.empty())
            continue;
        
        if (stmt_u == NORMAL) { // analyze WR dependency and store the read result
            
        }
    }
}

dependency_analyzer::~dependency_analyzer()
{
    for (int i = 0; i < tid_num; i++) 
        delete[] dependency_graph[i];
    delete[] dependency_graph;
}
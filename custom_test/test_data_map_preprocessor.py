def test_process_dag():
    
    my_process_dag = process_dag()
    my_process_dag.add_node('input0', 'input')
    my_process_dag.add_node('process0', 'to_control')
    my_process_dag.add_edge('input0', 'process0')
    for i in my_process_dag['input0']:
        print(i)
        

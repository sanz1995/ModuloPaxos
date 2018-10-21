Code.require_file("#{__DIR__}/send_adicional.exs")


defmodule Aceptador do

	def init(num_instancia) do
	    Node.spawn_link(node(), __MODULE__, :aceptador, [num_instancia,0,0,0])
	end


	def aceptador(num_instancia,n_p,n_a,v_a) do
		
		receive do
            {:prepara,n,origin} -> 
            	if n > n_p do
                    Send.con_nodo_emisor({:paxos, origin},{:prepare_ok,n,n_a,v_a,num_instancia})

                    aceptador(num_instancia,n,n_a,v_a)
                else
                    aceptador(num_instancia,n_p,n_a,v_a)
                end

            {:acepta,n,v,origin} -> 
            	if n >= n_p do

                    Send.con_nodo_emisor({:paxos, origin},{:acepta_ok,n,num_instancia})

                    aceptador(num_instancia,n,n,v)
                else
                    Send.con_nodo_emisor({:paxos, origin},{:acepta_reject,n_p,num_instancia})
                    aceptador(num_instancia,n_p,n_p,v_a)
                end
            {:decidido,origin} -> Send.con_nodo_emisor({:paxos, origin},{:recibido,num_instancia})


        end




        aceptador(num_instancia,n_p,n_a,v_a)


	end







end
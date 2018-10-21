Code.require_file("#{__DIR__}/send_adicional.exs")


defmodule Proponente do
	


    @t 25
	def init(servidores, num_instancia, v, paxos) do
	    Node.spawn_link(node(), __MODULE__, :proponer, [servidores,num_instancia, 0, v, paxos])
	end


	def proponer(servidores, num_instancia, n, v, paxos) do
        #IO.puts("----------------------------------------------------------")

        #IO.inspect(servidores)




        #ENVIAR PREPARA A TODOS LOS SERVIDORES
        Enum.map(servidores, fn x -> Send.con_nodo_emisor({:paxos, x},{:prepara,n,num_instancia,paxos}) end)


        #ESPERAR RESPUESTA DE UNA MAYORÍA
        {acuerdo,v,mayor_n} = esperar_prepare_ok((length(servidores)/2),0,0,v)
        


        if acuerdo do
            IO.puts("Acuerdo en Prepara")

            #ENVIAR ACEPTA A TODOS LOS SERVIDORES
            Enum.map(servidores, fn x -> Send.con_nodo_emisor({:paxos, x},{:acepta,n,v,paxos,num_instancia}) end)

            #ESPERAR RESPUESTA DE UNA MAYORÍA
            if esperar_acepta_ok((length(servidores)/2),0) do
                
                IO.puts("Acuerdo en Acepta")

                #ENVIAR DECIDIDO A TODOS LOS SERVIDORES
                Enum.map(servidores, fn x -> Send.con_nodo_emisor({:paxos, x},{:decidido,v,num_instancia}) end)


                IO.puts("Terminado")
                #INFORMAR AL NODO PAXOS DE QUE HA TERMINADO EL PROCESO
                Send.con_nodo_emisor({:paxos, paxos},{'EXIT',self(),:ok})
            else


                IO.inspect({"Repetir",max(mayor_n,n)+1})
                proponer(servidores, num_instancia,max(mayor_n,n)+1,v,paxos)
            end
        else
            IO.inspect({"Repetir",max(mayor_n,n)+1})
            proponer(servidores, num_instancia,max(mayor_n,n)+1,v,paxos)
        end


    end







    def esperar_prepare_ok(numMayoria,count,mayor_n_a,v) do

        if count > numMayoria do
            {true,v,mayor_n_a}
        else
            receive do
                {:prepare_ok,n,n_a,v_a} -> 
                    if n_a!=0 do
                        if n_a > mayor_n_a do
                            esperar_prepare_ok(numMayoria,count + 1,n_a,v_a)
                        else
                            esperar_prepare_ok(numMayoria,count + 1,mayor_n_a,v)
                        end
                    else
                        esperar_prepare_ok(numMayoria,count + 1,mayor_n_a,v)
                    end

                {:prepare_reject,n_p} -> 
                    #IO.puts("reject")
                    if n_p > mayor_n_a do
                        esperar_prepare_ok(numMayoria,count,n_p,v)
                    else
                        esperar_prepare_ok(numMayoria,count,mayor_n_a,v)
                    end
                    #Comprobar si ya no se puede
            after @t ->
                IO.puts("No hay acuerdo")
                {false,v,mayor_n_a}
            end    
        end   
    end



    def esperar_acepta_ok(numMayoria,count) do

        receive do
            {:acepta_ok,n} -> 
                if (count+1)>numMayoria do
                    true
                else
                    esperar_acepta_ok(numMayoria,count + 1)
                end

            {:acepta_reject,n_p} -> 
                #IO.puts("reject")
                esperar_acepta_ok(numMayoria,count)
                #Comprobar si ya no se puede
        after @t ->
            IO.puts("No hay acuerdo")
            false
        end       
    end


    
end
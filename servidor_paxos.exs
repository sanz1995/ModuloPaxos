# Servidor Paxos con variante de particionamiento por cambio de send y recepcion
# Que posibilita particiones dinámicas en ejecución

# Compilar y cargar ficheros con modulos necesarios
Code.require_file("#{__DIR__}/nodo_remoto.exs")
Code.require_file("#{__DIR__}/send_adicional.exs")
Code.require_file("#{__DIR__}/proponente.exs")
Code.require_file("#{__DIR__}/aceptador.exs")

defmodule ServidorPaxos do

    @moduledoc """
        modulo del servicio de vistas
    """

    # Tipo estructura de datos que guarda el estado del servidor Paxos
    # COMPLETAR  con lo campos necesarios para gestionar
    # el estado del gestor de vistas






    defstruct   fiabilidad: :fiable,
                n_mensajes: 0,
                servidores: [],
                yo: :ninguno ,
                nodos_accesibles: [], # por defecto, nodo no pertenece partición
                decididos: %{},
                proponentes: %{},
                aceptadores: %{},
                hechos: %{},
                hecho: 0
              #completar esta estructura de datos con lo que se necesite

    @timeout 50


    @doc """
        Crear y poner en marcha un servidor Paxos
        Los nombres Elixir completos de todos los servidores están en servidores
        Y el nombre de máquina y nombre nodo Elixir de este servidor están en 
        host y nombre_nodo
        Devuelve :  :ok.

    """
    @spec start([atom], String.t, String.t) :: atom
    def start(servidores, host, nombre_nodo) do
        nodo = NodoRemoto.start(host, nombre_nodo,__ENV__.file, __MODULE__)
        Process.flag(:trap_exit, true)
        Node.spawn_link(nodo, __MODULE__, :init, [servidores, nodo])
        
        # eliminar mensajes de terminacion del port utilizado para
        # ejecución de ssh en NodoRemoto (devuelven {:EXIT, port, :normal})
        vaciar_buzon() 
        nodo
    end

    @doc """
        Parar un servidor Paxos
        Devuelve : :ok
    """
    @spec stop(node) :: :ok
    def stop(nodo_paxos) do
        NodoRemoto.stop(nodo_paxos)
        vaciar_buzon()
    end

    @doc """
        Vaciar buzón del proceso en curso (denominado flush en iex)
        Devuelve : :ok
    """
    @spec vaciar_buzon() :: :ok
    def vaciar_buzon() do
        receive do 
            _ -> vaciar_buzon()
        after   0 -> :ok
        end
    end

    @doc """
        Petición de inicio de proceso de acuerdo para una instancia nu_instancia
        con valor propuesto valor, al servidor Paxos  nodo_paxos
        Devuelve de inmediato:  :ok
    """
    @spec start_instancia(node, non_neg_integer, String.t) :: :ok
    def start_instancia(nodo_paxos, nu_instancia, valor) do
        
        #spawn(proponer(1,2))
        Send.con_nodo_emisor({:paxos, nodo_paxos},{:proponer,nu_instancia,valor,self()})

        receive do
            mensaje -> mensaje
        after @timeout ->
            start_instancia(nodo_paxos, nu_instancia, valor)
        end
        #spawn(proponer(nu_instancia,valor))
        #receive do Respuesta -> :ok end
        # VUESTRO CODIGO AQUI
        :ok
    end

    @doc """
        La aplicación quiere saber si el servidor nodo_paxos opina que
        la instancia nu_instancia ya se ha decidido.
        Solo debe mirar el servidor NodoPaxos sin contactar con ningún otro
        Devuelve : {Decidido :: bool, valor}
    """
    @spec estado(node, non_neg_integer) :: {boolean, String.t}
    def estado(nodo_paxos, nu_instancia) do
        
        #{Decidido,}
        
        #IO.puts("comprobando")

        Send.con_nodo_emisor({:paxos, nodo_paxos},{:decidido?,nu_instancia,self()})
        receive do
            mensaje -> IO.inspect({nu_instancia,mensaje})
                mensaje
        after @timeout ->
            {false,:ficticio}    
        end
        #:ok
    end

    @doc """
        La aplicación en el servidor nodo_paxos ya ha terminado
        con todas las instancias <= nu_instancia
        Mirar comentarios de min() para más explicaciones
        Devuelve :  :ok
    """
    @spec hecho(node, non_neg_integer) :: {boolean, String.t}
    def hecho(nodo_paxos, nu_instancia) do
        
        # VUESTRO CODIGO AQUI

        Send.con_nodo_emisor({:paxos, nodo_paxos},{:hecho,nu_instancia,self()})
        receive do
            mensaje -> mensaje
        end

    end

    @doc """
        Aplicación quiere saber el máximo número de instancia que ha visto
        este servidor NodoPaxos
        Devuelve : NuInstancia
    """
    @spec maxi(node) :: non_neg_integer
    def maxi(nodo_paxos) do
        
        # VUESTRO CODIGO AQUI

        Send.con_nodo_emisor({:paxos, nodo_paxos},{:max,self()})
        receive do
            mensaje -> mensaje
        end

    end

    @doc """
        Minima instancia vigente de entre todos los nodos Paxos
        Se calcula en función Aceptador.modificar_state_inst_y_hechos
        Devuelve : nu_instancia = hecho + 1
    """
    @spec mini(node) :: non_neg_integer
    def mini(nodo_paxos) do
        
        # VUESTRO CODIGO AQUI

        Send.con_nodo_emisor({:paxos, nodo_paxos},{:min,self()})
        receive do
            mensaje -> mensaje
        end

    end

    @doc """
        Cambiar comportamiento de comunicación del Nodo Elixir a NO FIABLE
    """
    @spec comm_no_fiable(node) :: :comm_no_fiable
    def comm_no_fiable(nodo_paxos) do       
        Send.con_nodo_emisor({:paxos, nodo_paxos}, :comm_no_fiable)
    end

    @doc """
        Limitar acceso de un Nodo a solo otro conjunto de Nodos,
        incluido este nodo de control
        Para SIMULAR particiones de red
    """
    @spec limitar_acceso(node, [node]) :: :ok
    def limitar_acceso(nodo_paxos, otros_nodos) do
        Send.con_nodo_emisor({:paxos, nodo_paxos},
                                    {:limitar_acceso, otros_nodos ++ [node()]})
        :ok
    end

    @doc """
        Hacer que un servidor Paxos deje de escuchar cualquier mensaje,
        salvo 'escucha'
    """
    @spec ponte_sordo(node) :: :ponte_sordo
    def ponte_sordo(nodo_paxos) do
        Send.con_nodo_emisor({:paxos, nodo_paxos},:ponte_sordo)
    end

    @doc """
        Hacer que un servidor Paxos deje de escuchar cualquier mensaje,
        salvo 'escucha'
    """
    @spec escucha(node) :: :escucha
    def escucha(nodo_paxos) do
        IO.inspect({"Escuchar",nodo_paxos})
        Send.con_nodo_emisor({:paxos, nodo_paxos},:escucha)
    end

    @doc """
        Obtener numero de mensajes recibidos en un nodo
    """
    @spec n_mensajes(node) :: non_neg_integer
    def n_mensajes(nodo_paxos) do
        Send.con_nodo_emisor({:paxos, nodo_paxos},{:n_mensajes, self()})
        receive do Respuesta -> Respuesta end
    end




    #------------------- Funciones privadas

    # La primera debe ser def (pública) para la llamada :
    # spawn_link(__MODULE__, init,[...])
    def init(servidores, yo) do
        Process.register(self(), :paxos)

        #### VUESTRO CODIGO DE INICIALIZACION


        #spawn(proponer(1,2))
        estado = %ServidorPaxos{servidores: servidores,yo: yo, fiabilidad: :fiable}

        estado = %{estado | hechos: Enum.reduce(servidores,%{},fn(x,acc) -> Map.put(acc, x, 0) end)}

        #IO.inspect(estado.hechos)

        bucle_recepcion(estado)
    end





    defp bucle_recepcion(estado) do


        #IO.inspect(estado.servidores)

        # Obtener mensaje si viene de misma particion, y procesarlo
        case filtra_recepcion(estado.nodos_accesibles) do
            :invalido ->    # se ignora este tipo de mensaje
                #bucle_recepcion(estado)
                bucle_recepcion(estado)

            :comm_no_fiable    ->
                estado = poner_no_fiable(estado)
                bucle_recepcion(estado)

            :comm_fiable       ->
                estado = poner_fiable(estado)
                bucle_recepcion(estado)
                
            {:es_fiable, pid} ->    
                #????????????

                bucle_recepcion(estado)

            {:limitar_acceso, nodos} ->
                estado = %{estado | nodos_accesibles: nodos}
                bucle_recepcion(estado)    

            {:n_mensajes, pid} ->
                #?????????
                bucle_recepcion(estado)

            {:decidido?, n,origin} ->
                #?????????
                v = estado.decididos[n]
                #IO.inspect(v)

                if v == nil do
                    send(origin,{false,:ficticio})
                else
                    send(origin,{true,v})
                end


                bucle_recepcion(estado)
            {:max,origin} ->

                keys = Map.keys(estado.decididos)

                if(length(keys) == 0) do
                    send(origin,0)
                else
                    send(origin,Enum.max(Map.keys(estado.decididos)))
                end
                #IO.in
                bucle_recepcion(estado)
            {:min,origin} ->

                values = Map.values(estado.hechos)

                if(length(values) == 0) do
                    send(origin,1)
                else
                    send(origin,Enum.min(values)+1)
                end

                bucle_recepcion(estado)

            {:hecho,n,origin} ->

                if estado.hechos[estado.yo] < n do
                    Enum.map(estado.servidores, fn x -> Send.con_nodo_emisor({:paxos, x},{:actualizar_hecho,estado.yo,n}) end)
                end
                send(origin,true)
                
                bucle_recepcion(estado)


            {:actualizar_hecho, node, n} ->
                estado = %{estado | hechos: Map.put(estado.hechos,node,n)}
                #IO.inspect(estado.hechos)

                min = Enum.min(Map.values(estado.hechos))
                keys_hechos = Enum.filter(Map.keys(estado.decididos),fn (x) -> x <= min end)

                estado = %{estado | decididos: Map.drop(estado.decididos,keys_hechos)}

                IO.inspect(estado.decididos)

                bucle_recepcion(estado)

            
            :ponte_sordo -> 
                IO.inspect({"Estoy sordo",estado.yo})
                espero_escucha(estado);

            {'EXIT', _pid, dato_devuelto} -> 
                # Cuando proceso proponente acaba
    
                # VUESTRO CODIGO AQUI

                IO.puts("FIN")
                #estado = %{estado | proponente: nil}
                

                bucle_recepcion(estado)

            # mensajes para proponente y aceptador del servidor local
            mensaje -> simula_fallo_mensj_prop_y_acep(mensaje,estado)
            
        end
    end
    
    defp filtra_recepcion(nodos_accesibles) do
        receive do
            {nodo_emisor, mensaje} ->
                case nodos_accesibles do
                    [] -> mensaje # no hay limitacion red de este nodo receptor 
                    nodos_acc ->
                        # nodo emisor esta en misma particion de nodo receptor
                        if Enum.member?(nodos_acc, nodo_emisor), do: mensaje,
                        # si NO lo esta, el mensaje NO se admite
                        else: :invalido  # nodo emisor NO es accesible
                end
            otro_msj ->
                exit("Error: función filtra_recepcion, mensaje : #{otro_msj}")
        end
    end

    defp espero_escucha(estado) do
        #IO.puts("#{node()} : Esperando a recibir escucha")
        receive do
            {origin,:escucha} ->
                IO.puts("#{node()} : Salgo de la sordera !!")
                bucle_recepcion(estado)
            
            _resto ->
                espero_escucha(estado)
        end
    end
    
    defp simula_fallo_mensj_prop_y_acep(mensaje,estado) do

        fiable = es_fiable?(estado)
        #IO.inspect({"Fiabilidad",fiable})
            # utilizamos el modulo de numeros aleatorios de Erlang "rand"
        aleatorio = :rand.uniform(1000)
        
        #si no fiable, eliminar mensaje con cierta aleatoriedad
        if  ((not fiable) and (aleatorio < 200)) do 
            bucle_recepcion(estado);
                  
        else  # Y si lo es tratar el mensaje recibido correctamente
            gestion_mnsj_prop_y_acep(mensaje,estado)
        end
    end

    defp gestion_mnsj_prop_y_acep( mensaje,estado) do

        # VUESTRO CODIGO AQUI

        IO.inspect(mensaje)

        case mensaje do    

            #Empezar propuesta
            {:proponer,num_instancia,v,origin} -> 

                IO.inspect({"Proponiendo",num_instancia,v})

                send(origin,:ok)


                #Inicializar proponente para la nueva instancia
                if estado.proponentes[num_instancia] == nil do
                    #estado = %{estado | proponente: Proponente.init(estado.servidores,num_instancia, v, estado.yo)}
                    estado = %{estado | proponentes: Map.put(estado.proponentes,num_instancia,Proponente.init(estado.servidores,num_instancia, v, estado.yo))}
                    bucle_recepcion(estado)
                else
                    #Ni caso
                    bucle_recepcion(estado)
                end

            #Mesajes para el aceptador
            {:prepara, n,num_instancia,origin} -> 
                #Inicializar aceptadores para la nueva instancia
                if(estado.aceptadores[num_instancia] == nil) do
                    #estado = %{estado | aceptador: Aceptador.init(num_instancia)}
                    estado = %{estado | aceptadores: Map.put(estado.aceptadores,num_instancia,Aceptador.init(num_instancia))}
                    
                    send(estado.aceptadores[num_instancia],{:prepara,n,origin})
                    bucle_recepcion(estado)
                else
                    send(estado.aceptadores[num_instancia],{:prepara,n,origin})
                    bucle_recepcion(estado)
                end

            {:acepta,n,v,origin,num_instancia} -> 
                #IO.puts("acep")

                if(estado.aceptadores[num_instancia] == nil) do
                    estado = %{estado | aceptadores: Map.put(estado.aceptadores,num_instancia,Aceptador.init(num_instancia))} 
                    send(estado.aceptadores[num_instancia],{:acepta,n,v,origin})
                    bucle_recepcion(estado)
                else
                    send(estado.aceptadores[num_instancia],{:acepta,n,v,origin})
                    bucle_recepcion(estado)
                end


            {:decidido,v,origin,num_instancia} -> 

                if(estado.aceptadores[num_instancia] == nil) do
                    estado = %{estado | aceptadores: Map.put(estado.aceptadores,num_instancia,Aceptador.init(num_instancia))}

                    send(estado.aceptadores[num_instancia],{:decidido,origin})
                else
                    send(estado.aceptadores[num_instancia],{:decidido,origin})
                end


                

                if estado.decididos[num_instancia] == nil do
                    estado = %{estado | decididos: Map.put(estado.decididos,num_instancia,v)}
                    IO.inspect({"decidido",num_instancia,v})

                    if estado.hecho < num_instancia do
                        #estado = %{estado | hecho: num_instancia}
                        bucle_recepcion(estado)
                    else
                        bucle_recepcion(estado)
                    end
                else
                    #IO.inspect({"Instancia ya había sido decidida",num_instancia,v})
                    bucle_recepcion(estado)
                end

            {:recibido,num_instancia} -> send(estado.proponentes[num_instancia],:recibido)

            #Mensajes para el proponente
            {:prepare_ok,n,n_a,v_a,num_instancia} -> 
                send(estado.proponentes[num_instancia],{:prepare_ok,n,n_a,v_a})
                bucle_recepcion(estado)

            {:prepare_reject,n_p,num_instancia} -> 
                send(estado.proponentes[num_instancia],{:prepare_reject,n_p})
                bucle_recepcion(estado)

            {:acepta_ok,n,num_instancia} -> 
                send(estado.proponentes[num_instancia],{:acepta_ok,n})
                bucle_recepcion(estado)

            {:acepta_reject,n_p,num_instancia} -> 
                send(estado.proponentes[num_instancia],{:acepta_reject,n_p})
                bucle_recepcion(estado)


        end
        

        bucle_recepcion(estado)
        
    end



    defp actualizarV(estado,n,n_a,v_a) do
        #definir cual es el mayor n visto en mensajes acepta
        if estado.may_n_a <= n_a do

            estado = %{estado | may_n_a: n_a}

            #Si n es el mas grande hasta el momento, utilizar valor propio, sino sobreescribir
            if n_a != 0 do
                IO.inspect("V_actualizada")
                estado = %{estado | v: v_a}
                estado
            end
        end
        estado
    end





    defp poner_fiable(estado) do
        %{estado | fiabilidad: :fiable}
    end

    defp poner_no_fiable(estado) do
        %{estado | fiabilidad: :nofiable}
    end

    defp es_fiable?(estado) do
        estado.fiabilidad==:fiable
    end

end

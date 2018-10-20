# Servidor Paxos con variante de particionamiento por cambio de send y recepcion
# Que posibilita particiones dinámicas en ejecución

# Compilar y cargar ficheros con modulos necesarios
Code.require_file("#{__DIR__}/nodo_remoto.exs")
Code.require_file("#{__DIR__}/send_adicional.exs")

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
                decididos: [],
                n_p: 0,
                n_a: 0,
                v_a: 0,
                v: 0,



                may_n_a: 0,
                enviados_a: false,
                n_prepara: 0,
                n_acepta: 0,
                enviados_d: false,
                n_rejected: 0,
                repetido: false
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
        Send.con_nodo_emisor({:paxos, nodo_paxos},{:proponer,nu_instancia,valor})

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
        
        IO.puts("comprobando")

        Send.con_nodo_emisor({:paxos, nodo_paxos},{:decidido?,nu_instancia,self()})
        receive do
            {n,v} -> {true,v}
        end
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

    end

    @doc """
        Aplicación quiere saber el máximo número de instancia que ha visto
        este servidor NodoPaxos
        Devuelve : NuInstancia
    """
    @spec maxi(node) :: non_neg_integer
    def maxi(nodo_paxos) do
        
        # VUESTRO CODIGO AQUI

    end

    @doc """
        Minima instancia vigente de entre todos los nodos Paxos
        Se calcula en función Aceptador.modificar_state_inst_y_hechos
        Devuelve : nu_instancia = hecho + 1
    """
    @spec mini(node) :: non_neg_integer
    def mini(nodo_paxos) do
        
        # VUESTRO CODIGO AQUI

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
        estado = %ServidorPaxos{servidores: servidores,yo: yo}

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
                poner_no_fiable()
                bucle_recepcion(estado)

            :comm_fiable       ->
                poner_fiable()
                bucle_recepcion(estado)
                
            {:es_fiable, Pid} ->    
                #????????????

                bucle_recepcion(estado)

            {:limitar_acceso, Nodos} ->
                #????????????
                bucle_recepcion(estado)    

            {:n_mensajes, Pid} ->
                #?????????
                bucle_recepcion(estado)

            {:decidido?, n,origin} ->
                #?????????
                send(origin,Enum.find(estado.decididos, fn(x) -> match?({n, _}, x) end))


                bucle_recepcion(estado)
            
            :ponte_sordo -> espero_escucha(estado);

            {'EXIT', _pid, dato_devuelto} -> 
                # Cuando proceso proponente acaba
    
                # VUESTRO CODIGO AQUI

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
        IO.puts("#{node()} : Esperando a recibir escucha")
        receive do
            :escucha ->
                IO.puts("#{node()} : Salgo de la sordera !!")
                bucle_recepcion(estado)
            
            _resto -> espero_escucha(estado)
        end
    end
    
    defp simula_fallo_mensj_prop_y_acep(mensaje,estado) do

        fiable = es_fiable?()
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




        case mensaje do    
            {:proponer,n,v} -> 

                
                
                Enum.map(estado.servidores, fn x -> Send.con_nodo_emisor({:paxos, x},{:prepara,n,estado.yo}) end)

                estado = %{estado | v: v}



                estado = %{estado | n_prepara: 0}
                estado = %{estado | enviados_a: false}
                estado = %{estado | n_acepta: 0}
                estado = %{estado | enviados_d: false}
                estado = %{estado | n_rejected: 0}
                estado = %{estado | repetido: false}


                bucle_recepcion(estado)

            {:prepara, n,origin} -> 

                #IO.inspect({"Respondiendo",estado.yo,origin })
                if n > estado.n_p do
                    estado = %{estado | n_p: n}
                    Send.con_nodo_emisor({:paxos, origin},{:prepare_ok,n,estado.n_a,estado.v_a})
                    bucle_recepcion(estado)
                else
                    Send.con_nodo_emisor({:paxos, origin},{:prepare_reject,estado.n_p})
                end
            {:prepare_ok,n,n_a,v_a} -> 


                estado = %{estado | n_prepara: estado.n_prepara+1}
                IO.inspect({estado.n_prepara,estado.yo})


                #Comprobar si hay que actualizar v
                estado = actualizarV(estado,n,n_a,v_a)



                if estado.n_prepara >= (length(estado.servidores)/2) do
                    #Mayoría

                    #Comprobar que no se haya enviado ya
                    if estado.enviados_a == false do
                        IO.inspect("Mayoria")
                        Enum.map(estado.servidores, fn x -> Send.con_nodo_emisor({:paxos, x},{:acepta,n,estado.v,estado.yo}) end)
                        estado = %{estado | enviados_a: true}
                        bucle_recepcion(estado)
                    end
                end



                bucle_recepcion(estado)


            {:prepare_reject,n_p} -> 
                IO.inspect({"rejected",estado.yo})
                estado = %{estado | n_rejected: estado.n_rejected+1}

                #Si mayoría rejected repito la propuesta con otro numero de instancia
                if estado.n_rejected >= (length(estado.servidores)/2) do
                    #Mayoría

                    #Comprobar que no se haya enviado ya
                    if estado.repetido == false do
                        IO.inspect({"Repitiendo",estado.yo})
                        Send.con_nodo_emisor({:paxos, estado.yo},{:proponer,estado.n_p+1,estado.v})
                        estado = %{estado | repetido: true}
                        bucle_recepcion(estado)
                    end
                end
                bucle_recepcion(estado)


            {:acepta,n,v,origin} -> 


                if n >= estado.n_p do
                    estado = %{estado | n_p: n}
                    estado = %{estado | n_a: n}
                    estado = %{estado | v_a: v}

                    Send.con_nodo_emisor({:paxos, origin},{:acepta_ok,n})

                    bucle_recepcion(estado)
                else
                    Send.con_nodo_emisor({:paxos, origin},{:acepta_reject,estado.n_p})
                end

            {:acepta_ok,n} -> 
                estado = %{estado | n_acepta: estado.n_acepta+1}

                if estado.n_acepta >= (length(estado.servidores)/2) do
                    #Mayoría
                    
                    if estado.enviados_d == false do
                        Enum.map(estado.servidores, fn x -> Send.con_nodo_emisor({:paxos, x},{:decidido,estado.v}) end)
                        estado = %{estado | enviados_d: true}
                        bucle_recepcion(estado)
                    end


                end
                bucle_recepcion(estado)

            {:acepta_reject,n_p} -> 
                IO.inspect({"Acept rejected--------------------------------------------",estado.yo})
                estado = %{estado | n_rejected: estado.n_rejected+1}

                #Si mayoría rejected repito la propuesta con otro numero de instancia
                if estado.n_rejected >= (length(estado.servidores)/2) do
                    #Mayoría

                    #Comprobar que no se haya enviado ya
                    if estado.repetido == false do
                        IO.inspect({"Repitiendo desde acept",estado.yo})
                        Send.con_nodo_emisor({:paxos, estado.yo},{:proponer,estado.n_p+1,estado.v})
                        estado = %{estado | repetido: true}
                        bucle_recepcion(estado)
                    end
                end
                bucle_recepcion(estado)

            {:decidido,v} -> 
                estado = %{estado | decididos: estado.decididos ++ [{estado.n_p,v}]}
                
                IO.inspect({"decidido",estado.n_p,v})
                
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




    defp poner_fiable() do
        :ok
    end

    defp poner_no_fiable() do
        :ok
    end

    defp es_fiable?() do
        true
    end

end

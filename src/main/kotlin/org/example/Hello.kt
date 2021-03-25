package org.example

import nanomsg.bus.BusSocket
import kotlinx.coroutines.*
import kotlin.random.Random

fun main(args: Array<String>) {
    runBlocking {
        println("Hello, World ${args[0]}")

        // Create sockets
        val sender = BusSocket()
        val listener = BusSocket()

        // Connect
        listener.bind(args[0]) // My IP ex.: tcp://*:4001

        sender.connect(args[1]) // Other IP
        sender.connect(args[2]) // Other IP

        // Term
        var term = 0

        // Election Votes
        var votes = 0

        // States: Leader, Candidate, Follower
        var states = "Follower";

        var myLeader = ""

        var firstTime = true

        val printjob = GlobalScope.launch {
            var ft = true;
            while(true){
                if(ft){
                    ft = false
                    delay(10000)
                }else{
                    delay(1000)
                }
                if(states == "Leader"){
                    myLeader = ""
                }
                println("===================================")
                println("Node = ${args[0]}")
                println("Term = ${term}")
                println("Leader = ${myLeader}")
                println("===================================")
                println("")

            }
        }


        fun updateTermAndStartElection(){
            println("inciou eleição -------------------------------------")
            // Change state
            states="Candidate"
            term+=1
            votes+=1
            sender.send("request_vote $term ${args[0]}")

            GlobalScope.launch {
                delay(310L)
                if(votes>=2){
                    states="Leader"
                    votes=0
                    println("Sou Novo Leader ${args[0]}")
                    while(true){
                        sender.send("heart-beat ${term} ${args[0]}")
                        delay(100)
                    }

                }else{
                    states="Follower"
                    votes=0
                }
            }
        }

        fun getElectionJob (): Job { // Timer
            val electionStart = if(firstTime) GlobalScope.launch{
                firstTime = false
                var server = args[0].split(":")[2]
                if(server == "4001"){
                    delay(5000)
                }else if(server == "4002"){
                    delay(2500)
                }else if(server == "4003"){
                    delay(0)
                }

                val timer= Random.nextLong(150,300)
                delay(timer)
                updateTermAndStartElection()

            } else GlobalScope.launch {
                val timer= Random.nextLong(150,300)
                delay(timer)
                updateTermAndStartElection()
            }
            return electionStart
        }



        val listenToMessageJob = GlobalScope.launch {




            while (true) {
                var electionStart = getElectionJob() // iniciar timer



                if(states == "Leader" || states == "Candidate"){
                    electionStart.cancelAndJoin()
                }
                //Hearth-beat, vote and request_vote
                val message = listener.recvString()

                println("\n recebeu $message \n")

                // Reset time when message come in
                electionStart.cancelAndJoin()


                //padrão: comando args[]
                val messageSplit = message.split(" ")


                if(messageSplit[0]=="request_vote"){

                    if(states == "Leader"){
                        sender.send("setLeader ${args[0]} ${term}")
                    }

                    val comingTerm = Integer.parseInt(messageSplit[1])

                    if(myLeader != "" && comingTerm < term ){
                        // Se eu recebi mensagem de alguem com o termo menor, então eu ignoro e espero meu
                        // meu tempo estourar para iniciar uma eleição
                        continue
                    }




                    if(states == "Candidate" && term <  comingTerm){
                        electionStart.cancelAndJoin()
                        myLeader = messageSplit[2]
                        term = comingTerm
                        states ="Follower"
                        sender.send("vote ${messageSplit[2]}")
                        continue
                    }
                    if(states == "Candidate" && term ==  comingTerm){
                        continue
                    }
                    if(states == "Follower"){
                        if (term <  comingTerm){
                            sender.send("vote ${messageSplit[2]} eu sou o ${args[0]}")
                            electionStart.cancelAndJoin()
                            term = comingTerm
                            myLeader = messageSplit[2]

                            continue
                        }else{
                            updateTermAndStartElection()
                            continue
                        }
                    }
                }


                if(messageSplit[0]=="vote"){
                    if(messageSplit[1]==args[0]){ // se o voto for para mim, BUS
                        votes+=1
                        //println("add a new vote = $votes")
                        continue
                    }
                }


                if(messageSplit[0]=="heart-beat"){
                    myLeader = messageSplit[2]
                    electionStart.cancelAndJoin()
                    states = "Follower"
                    term = Integer.parseInt(messageSplit[1])
                    votes = 0
                }

                if(messageSplit[0] == "setLeader"){ // Quando eu peço voto a um lider e recebo setLeader
                    electionStart.cancelAndJoin()
                    myLeader = messageSplit[1]
                    states = "Follower"
                    votes = 0
                    term = Integer.parseInt(messageSplit[2])
                }

            }
        }
        listenToMessageJob.join()
        printjob.join()
    }
}




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
        listener.bind(args[0]) // My IP

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

        var cancelledElection = false


        val printjob = GlobalScope.launch {
            while(true){
                println("===================================")
                println("Node = ${args[0]}")
                println("Term = ${term}")
                println("Leader = ${myLeader}")
                println("===================================")
                println("")
                delay(1000)
            }
        }


        fun updateTermAndStartElection(){
            println("entrou aqui -------------------------------------")
            // Change state
            states="Candidate"
            term+=1
            votes+=1
            sender.send("request_vote $term ${args[0]}")
            GlobalScope.launch {
                delay(100L)
                if(votes>1){
                    cancelledElection=true
                    states="Leader"
                    votes=0

                    println("Sou Novo Leader ${args[0]}")
                    while(true){
                        sender.send("heart-beat ${term} ${args[0]}")
                        delay(50)
                    }

                }else{
                    states="Follower"
                    votes=0
                }
            }
        }

         GlobalScope.launch {
            val timer= Random.nextLong(150,300)
            println("2 === $timer")
            delay(timer)
            updateTermAndStartElection()
        }

        fun getElectionJob (): Job {
            val electionStart = if(firstTime) GlobalScope.launch{
                firstTime = false
                val timer= Random.nextLong(10000,15000) // tempo do terceiro iniciar
                println(timer)
                delay(timer)
                updateTermAndStartElection()

            } else GlobalScope.launch {
                val timer= Random.nextLong(150,5000)
                println("2 === $timer")
                delay(timer)
                updateTermAndStartElection()
            }
            return electionStart
        }



        val listenToMessageJob = GlobalScope.launch {
           var electionStart = getElectionJob()



            while (true) {


                if(cancelledElection){
                    electionStart.cancelAndJoin()
                }


                val message = listener.recvString()
                println("recebeu $message")
                // Reset time when message come in
                electionStart.cancelAndJoin()


                //println("listening  - ${listener.recvString()}")

                val messageSplit = message.split(" ")

                if(messageSplit[0]=="request_vote"){
                    // Reinicia processo de eleição
                    cancelledElection = false
                    val comingTerm = Integer.parseInt(messageSplit[1])

                    if(myLeader != "" && comingTerm < term ){
                        continue
                    }

                    // If you are a leader or candidate you cancel a new election
                    if(term > comingTerm){
                       // sender.send("vote ${messageSplit[2]}")
                      //  myLeader = messageSplit[2]
                       // term = comingTerm
                        electionStart.cancelAndJoin()
                        cancelledElection = true
                        println("Bora ver ")
                        continue
                    }


                    if(states == "Candidate" && term <  comingTerm){
                        // start a new timer to election
                        //electionStart = getElectionJob()
                        electionStart.cancelAndJoin()
                        myLeader = messageSplit[2]
                        term = comingTerm
                        //println("vote ${messageSplit[2]}")
                        sender.send("vote ${messageSplit[2]}")
                        continue
                    }
                    if(states == "Candidate" && term ==  comingTerm){

                       println("eaeeeeeeee")
                        continue
                    }
                    if(states == "Follower"){
                        if (term <  comingTerm){
                            sender.send("vote ${messageSplit[2]}")
                            //println("states == \"Follower\" new leader ${messageSplit[2]} ")
                            term = comingTerm
                            myLeader = messageSplit[2]
                            //println("vote ${messageSplit[2]}")
                            continue
                        }else{
                            println("Pois num caiu aqui no menor")
                            updateTermAndStartElection()
                            continue
                        }
                    }
                }


                if(messageSplit[0]=="vote"){
                    if(messageSplit[1]==args[0]){
                        votes+=1
                        println("add a new vote = $votes")
                        continue
                    }
                }
                if(messageSplit[0]=="heart-beat"){
                    myLeader = messageSplit[2]
                    cancelledElection = true
                    electionStart.cancelAndJoin()
                }
                if(cancelledElection == false) {
                    electionStart = getElectionJob()
                }

            }
        }

        listenToMessageJob.join()
        printjob.join()
    }
}




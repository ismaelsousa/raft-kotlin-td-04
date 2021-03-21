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

        var myLeader:String
        var firstTime = true

        fun updateTermAndStartElection(timer:Long){
            // Change state
            states="Candidate"
            term+=1
            votes+=1
            sender.send("request_vote $term ${args[0]}" )
            println("TIMEOUT ${timer} | Start-Election")
        }

        fun getElectionJob (): Job {
            val electionStart = if(firstTime) GlobalScope.launch{
                firstTime = false
                val timer= Random.nextLong(10000,10999)
                delay(timer)
                updateTermAndStartElection(timer)

            } else GlobalScope.launch {
                val timer= Random.nextLong(100,999)
                delay(timer)
                updateTermAndStartElection(timer)
            }
            return electionStart
        }

        val listenToMessageJob = GlobalScope.launch {
            // Election Job
            var electionStart = getElectionJob()

            var cancelledElection = false

            while (true) {


                if(cancelledElection){}

                // If you are a leader or candidate you cancel a new election
                if(states!="Follower"){
                    cancelledElection = true
                    electionStart.cancelAndJoin()
                }
                val message = listener.recvString()
                println("listening  - ${listener.recvString()}")

                val messageSplit = message.split(" ")
                if(messageSplit[0]=="request_vote"){

                    val comingTerm = Integer.parseInt(messageSplit[1])
                    if(states == "Candidate" && term <  comingTerm){
                        // start a new timer to election
                        term = comingTerm
                        electionStart = getElectionJob()
                        println("vote ${messageSplit[2]}")
                        sender.send("vote ${messageSplit[2]}")
                        continue
                    }
                    if(states == "Follower"){
                        println("vote ${messageSplit[2]}")
                        sender.send("vote ${messageSplit[2]}")
                    }
                }
                if(messageSplit[0]=="vote"){
                    if(messageSplit[2]==args[0]){
                        votes+=1
                        println("add a new vote = $votes")
                    }
                }
                if(message!="heart-beat"){
                    cancelledElection = true
                    electionStart.cancelAndJoin()
                }

                println("totalVotes: $votes")
                println("currentTerm: $term")
                println("state: $states")

            }
        }

        listenToMessageJob.join()
    }
}




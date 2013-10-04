open System
open System.Text
open ZeroMQ
open Newtonsoft.Json
open Newtonsoft.Json.FSharp

module Json =
    let convertFrom (converters:JsonConverter[]) v = 
        JsonConvert.SerializeObject(v,Formatting.Indented,converters)

    let convertTo (converters:JsonConverter[]) v = 
        JsonConvert.DeserializeObject<'t>(v,converters)

module Zmq =
    let send message (socket:ZmqSocket) =
        socket.Send(message, Encoding.Unicode) |> ignore

    let recieve (socket:ZmqSocket) =
        socket.Receive Encoding.Unicode

    let publisher addresss (zmqContext:ZmqContext) =
        let publisher = zmqContext.CreateSocket(SocketType.PUB)
        publisher.Bind addresss
        publisher
        
    let subscriber addresss (zmqContext:ZmqContext) =
        let subscriber = zmqContext.CreateSocket(SocketType.SUB)
        subscriber.SubscribeAll()
        subscriber.Connect addresss
        subscriber

    let publish converter socket message =
        let convertedMessage = converter message
        send convertedMessage socket 

    let subscribe converter socket =
        async {
            let message = recieve socket |> converter
            return message
        }
        
module Weather =
    type State =
    | Sunny
    | Rain of int

    type Status =
        { Place: string; Weather:State }

    let startServer (publish:Status->unit) = 
        async {
            let amountOfRain = new Random(DateTime.Now.Millisecond)

            while true do
                { Place = "Here"; Weather=Rain(amountOfRain.Next(100, 600)) }
                |> publish
        }

    let startClient (recieve:Async<Status>) =
        async {
            for i in 1 .. 10 do
                let! message = recieve
                match message.Weather with
                | Sunny -> printfn "It is Sunny at %s :)" message.Place
                | Rain(rain) ->  printfn "Rain is %imm at %s :(" rain message.Place
        }

[<EntryPoint>]
let main argv = 
    let converters : JsonConverter[] = [| UnionConverter<Weather.State>() |]
    use context = ZmqContext.Create()

    context
    |> Zmq.publisher "tcp://*:5556"
    |> Zmq.publish (Json.convertFrom converters)
    |> Weather.startServer
    |> Async.Start

    context
    |> Zmq.subscriber "tcp://localhost:5556"
    |> Zmq.subscribe (Json.convertTo converters)
    |> Weather.startClient
    |> Async.Start

    Console.ReadKey()
    |> ignore
    0

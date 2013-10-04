open System
open System.Text
open ZeroMQ
open Newtonsoft.Json
open Newtonsoft.Json.FSharp

module Json =
    let convertFrom v (converters:JsonConverter[]) = 
        JsonConvert.SerializeObject(v,Formatting.Indented,converters)

    let convertTo v (converters:JsonConverter[]) = 
        JsonConvert.DeserializeObject<'t>(v,converters)

module Zmq =
    let send message (socket:ZmqSocket) =
        socket.Send(message, Encoding.Unicode) |> ignore

    let recieve (socket:ZmqSocket) =
        socket.Receive Encoding.Unicode

    let publisher (zmqContext:ZmqContext)  addresss =
        let publisher = zmqContext.CreateSocket(SocketType.PUB)
        publisher.Bind addresss
        publisher
        
    let subscribe (zmqContext:ZmqContext) addresss =
        let subscriber = zmqContext.CreateSocket(SocketType.SUB)
        subscriber.SubscribeAll()
        subscriber.Connect addresss
        subscriber

module Weather =
    type State =
    | Sunny
    | Rain of decimal

    type Status =
        { Place: string; Weather:State }

    let startServer createPublisher = 
        async {
            use publisher = createPublisher "tcp://*:5556"
            let randomizer = new Random(DateTime.Now.Millisecond)

            while true do
                let next = randomizer.Next(100, 600).ToString()
                publisher
                |> Zmq.send next
        }

    let startClient createSubscriber =
        async {
            use subscriber = createSubscriber "tcp://localhost:5556"

            for i in 1 .. 10 do
                Zmq.recieve subscriber
                |> printfn "Recieved %i: %s" i
        }

[<EntryPoint>]
let main argv = 
    let converters : JsonConverter[] = [| UnionConverter<Weather.State>() |]
    use context = ZmqContext.Create()

    context
    |> Zmq.publisher 
    |> Weather.startServer
    |> Async.Start

    context
    |> Zmq.subscribe 
    |> Weather.startClient
    |> Async.Start

    Console.ReadKey()
    |> ignore
    0

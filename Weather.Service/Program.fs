open Microsoft.FSharp.Reflection
open System
open System.Linq
open System.Text
open ZeroMQ
open Newtonsoft.Json
open Newtonsoft.Json.Linq

module Json =
    let convertFrom (converters:JsonConverter[]) v = 
        JsonConvert.SerializeObject(v,Formatting.Indented,converters)

    let convertTo (converters:JsonConverter[]) v = 
        JsonConvert.DeserializeObject<'t>(v,converters)

    type UnionConverter<'u>() =
        inherit JsonConverter()
        let union = typeof<'u>
        let cases  = union |> FSharpType.GetUnionCases
        let names  = 
            cases |> Array.map (fun c -> let nm = union.Name + "+" + c.Name
                                         union.FullName.Replace(union.Name,nm))

        override __.WriteJson(writer,value,serializer) =
            match value with
            | null -> nullArg "value"
            | data -> 
                let thing = value.GetType()
                let (caseInfo, values) = FSharpValue.GetUnionFields(data, union)
                writer.WriteStartObject()
                writer.WritePropertyName(caseInfo.Name)
                writer.WriteStartArray()
                values |> Seq.iter writer.WriteValue
                writer.WriteEndArray()
                writer.WriteEndObject()

        override __.ReadJson(reader,_,_,serializer) =
            let thingy = serializer.Deserialize(reader);
            let jObj = JObject.FromObject(thingy).First :?> JProperty
            let caseInfo = cases |> Seq.find (fun i -> i.Name = jObj.Name.Replace("_",""))
            let caseTypes = caseInfo.GetFields() |> Seq.map (fun i -> i.PropertyType)
            let args =
                jObj.Value
                |> Seq.map2 (fun i j -> (j.Value<string>(), i)) caseTypes
                |> Seq.map (fun (i,iType) -> 
                    match iType with
                    | t when t = typeof<string> -> i :> obj
                    | _ -> JsonConvert.DeserializeObject(i, iType))
                |> Seq.toArray
            FSharpValue.MakeUnion(caseInfo,args);

        override __.CanConvert(vType) = 
            (vType = union) || 
            (names |> Array.exists (fun n -> n = vType.FullName.Replace("_","")))

module Zmq =
    let send message (socket:ZmqSocket) =
        socket.Send(message, Encoding.UTF8) |> ignore

    let recieve (socket:ZmqSocket) =
        socket.Receive Encoding.UTF8

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
    | Fog of string * double

    type Status =
        { Place: string; Weather:State }

    let startServer (publish:Status->unit) = 
        async {
            let amountOfRain = new Random(DateTime.Now.Millisecond)

            while true do
                { Place = "Here"; Weather=Rain(amountOfRain.Next(100, 600)) }
                |> publish
                { Place = "Here"; Weather=Sunny }
                |> publish
                { Place = "Here"; Weather=Fog("Cheese",1.2) }
                |> publish
        }

    let startClient (recieve:Async<Status>) =
        async {
            for i in 1 .. 10 do
                let! message = recieve
                match message.Weather with
                | Sunny -> printfn "It is Sunny at %s :)" message.Place
                | Rain(amount) ->  printfn "Rain is %imm at %s :(" amount message.Place
                | Fog(name,amount) -> printfn "Fogness: %s, Amount: %f :|" name amount
        }

    let startStringClient (recieve:ZmqSocket) =
        async {
            for i in 1 .. 10 do
                let message = recieve.Receive(Encoding.UTF8)
                printfn "%s" message
        }

[<EntryPoint>]
let main argv = 
    let converters : JsonConverter[] = [| Json.UnionConverter<Weather.State>() |]
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

    context
    |> Zmq.subscriber "tcp://localhost:5556"
    |> Weather.startStringClient
    |> Async.Start

    Console.ReadKey()
    |> ignore
    0

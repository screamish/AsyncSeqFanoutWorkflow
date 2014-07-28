open FSharp.Control
open System

let doWorkInParallel bufferSize waitTimeout numberOfConsumers consumer input = async {
    if waitTimeout < 500 then invalidArg "waitTimeout" "For performance reasons the waitTimeout is limited to a minumum of 500ms"
    let buffer = BlockingQueueAgent<_>(bufferSize)
    let inputFinished = ref false

    let produce seq = async {
        do! seq |> AsyncSeq.iterAsync (Some >> buffer.AsyncAdd)
        do! buffer.AsyncAdd None
    }
            
    let consumeParallel degree f = async {
        let consume f = async {
            while not <| !inputFinished do
                try
                    let! data = buffer.AsyncGet(waitTimeout)
                    match data with
                    | Some i -> do! f i
                    // whichever consumer gets the end of the input flips the inputFinished ref so they'll all stop processing
                    | None -> inputFinished := true
                with | :? TimeoutException -> ()
        }

        do! [for slot in 1 .. degree -> consume f ]
            |> Async.Parallel
            |> Async.Ignore
    }
                        
    let startProducer = produce input
    let startConsumers = consumeParallel numberOfConsumers consumer
    
    return! [| startProducer; startConsumers |] |> Async.Parallel |> Async.Ignore
}

[<EntryPoint>]
let main argv = 
    let input = seq {1..40} |> AsyncSeq.ofSeq
    let action i = async {
        do! Async.Sleep 20
        //printfn "GET %d" i
        }
    let test buff n = async{
        let stopwatch = System.Diagnostics.Stopwatch.StartNew()
        do! doWorkInParallel buff 500 n action input
        printfn "Finished with %d consumers in %dms" n stopwatch.ElapsedMilliseconds
        return stopwatch.Elapsed
        }
    [for buff in 1..5 do
        for n in 1..5 do
            yield test buff n |> Async.RunSynchronously, buff, n]
    |> List.iter (fun (time, buff, n) -> printfn "%d:%d b:c, %f seconds" buff n time.TotalSeconds)
    |> ignore

    0 // return an integer exit code

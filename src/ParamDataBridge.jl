module ParamDataBridge


using JLD2, FileIO, Dates, DataFrames, CSV


export create_dataframe, save_dataframe!, save_data!, sync_data!, delete_data!, merge_data!, check_duplicates


function create_dataframe(folder_path::String)

    df_filename = "noise_dataframe.csv"
    if isfile(joinpath(dir_path, df_filename))
        noise_dataframe = CSV.read(joinpath(dir_path, df_filename), DataFrame)
    else
        noise_dataframe = DataFrame("Number of realizations" => Int[], "Number of harmonics" => Int[], "Number of time points" => Int[],  
            "ω0" => Float64[], "ωmax" => Float64[], "t0" => Float64[], "tmax" => Float64[], "PSD"=> String[], "Noise file name" => String[])

    return noise_dataframe
    end
end


function save_dataframe!(df, folder_path)

    df_filename="noise_dataframe.csv"
    CSV.write(joinpath(folder_path, df_filename), df)

end

function save_data!(noise, df, folder_path, metadata)

    df_filename="noise_dataframe.csv"

    # much more efficient to use JLD2 here!
    noise_filename = string(now(), ".jld2")
    @save joinpath(folder_path, noise_filename) noise

    push!(df, [metadata..., noise_filename])
    CSV.write(joinpath(folder_path, df_filename), df)

end


function sync_data!(df::DataFrame, folder_path::String)
    # Get the set of filenames from the DataFrame's 9th column
    expected_files = Set(df[:, 9])
    
    # List all files in the directory
    current_files = readdir(folder_path)
    
    # Delete files that are not in the DataFrame
    for file in current_files
        file_path = joinpath(folder_path, file)
        if !(file in expected_files) && isfile(file_path) && endswith(file_path, ".jld2")
            rm(file_path)
        end
    end
    
    # Delete DataFrame rows for which the file does not exist
    for i in size(df, 1):-1:1
        file_name = df[i, 9]
        if !(file_name in current_files)
            delete!(df, i)
        end
    end
end


function delete_data!(row, df, folder_path)

    delete!(df, row)
    sync_data!(df, folder_path)
    save_dataframe!(df, folder_path)
    
end



function merge_data!(row_indices::Vector{Int}, df::DataFrame, folder_path::String)
    
    if length(unique(row_indices)) != length(row_indices)
        error("The row indices must be different.")
        return
    end

    # Check compatibility of rows 2-8
    for col in 2:8
        if !all([df[row_indices[1], col] == df[row, col] for row in row_indices])
            error("The metadata of the rows doesn't match in the required entries.")
            return
        end
    end

    # Merge matrices
    merged_matrix = hcat([jldopen(joinpath(folder_path, df[row, 9]), "r") do file
        read(file, "noise")
        end for row in row_indices]...)


    # # check if this is more efficient
    # matrices = Matrix{Float64}[]
    # for row in row_indices
    #     file_path = joinpath(folder_path, df[row, 9])
    #     jldopen(file_path, "r") do file
    #         push!(matrices, read(file, "noise")) # assuming each file has a "noise" key
    #     end
    # end
    # merged_matrix = hcat(matrices...)
        
        
    # Save the merged matrix to a new file
    new_num_realizations = sum(df[row_indices, 1])
    metadata = [new_num_realizations; df[row_indices[1], 2:8]...]
    save_noise_data!(merged_matrix, df, folder_path, metadata)    

    # # Delete old rows
    # for row in reverse(sort(row_indices))
    #     delete!(df, row)
    # end

    # # Sync the DataFrame with the folder
    # sync_data!(df, folder_path)

end


function check_duplicates(row1::Int, row2::Int, df::DataFrame, dir_path::String)::Bool
    
    file1_path = joinpath(dir_path, df[row1, 9])
    file2_path = joinpath(dir_path, df[row2, 9])

    # Load the data from both files
    data1 = jldopen(file1_path, "r") do file
        read(file)
    end
    data2 = jldopen(file2_path, "r") do file
        read(file)
    end

    # Compare the data from the two files
    return data1 == data2
end


end # module

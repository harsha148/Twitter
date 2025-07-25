import axios from "axios";
import React, { useEffect, useState } from "react";
import UserCard from "./UserCard";
import { Box, Stack } from "@mui/material";

function Users({users,searchByUser}){
    // useEffect(()=>{
    //     if (searchBy==''){
    //         console.log('Getting Trending users');
    //         axios.get('http://localhost:8000/trendingusers')
    //         .then(response=>{
    //             console.log('Response received from trendingusers endpoint');
    //             setUsers(response.data);
    //         })
    //         .catch(error=>{
    //             console.log('Error occured while reaching endpoint trendingusers');
    //             console.log(error);
    //         })
    //     }
    // },[])
    return (
        <div>
            <Stack>
                {users.map((user)=>(
                    <UserCard user_name={user.user_name} user_id={user.user_id} verified={user.verified} description={user.description} searchByUser={searchByUser}/>
                ))}
            </Stack>
        </div>
    )
}

export default Users
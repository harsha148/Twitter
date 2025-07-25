import { VerifiedUser } from '@mui/icons-material'
import { Button, Typography } from '@mui/material'
import React from 'react'
import { TwitterFollowButton } from 'react-twitter-embed'

function UserCard({user_name, user_id,verified,description,searchByUser}){
    return(
        <Button variant='text' color='inherit' style={{justifyContent: "flex-start"}} onClick={()=>{console.log('Setting searchBy to user_id')
                searchByUser(user_id)
            }}>
            <Typography variant='subtitle1'>
                <b>{user_name}</b>
            </Typography>
            {verified && <VerifiedUser className="post__badge" />}
            <Typography variant='caption' sx={{display:'block'}}>
                {user_id}
            </Typography>
        </Button>
    )

}

export default UserCard
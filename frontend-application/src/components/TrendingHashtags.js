// import React,{ useEffect, useState } from 'react';
// import Hashtag from './Hashtag';
// import axios from 'axios';
// import { Stack, Box, Button } from '@mui/material';
// import "./Widgets.css";

// export default function TrendingHashtags(props) {
//     const [hashtags,setHashtags] = useState([])
//     useEffect(()=>{
//         axios.get('http://localhost:8000/hashtags')
//         .then(response=>{
//             console.log('Received response from get hashtags endpoint')
//             console.log(response)
//             setHashtags(response.data.slice(0,5))
//         })
//     },[])

//     return (
//         <div className="widgets">
//             <Box sx={{ width: '100%' }}>
//             <Stack spacing={2}>
//                 {hashtags.map((text)=>
//                     (
//                         <Button variant='text' color='inherit' style={{justifyContent: "flex-start"}}>
//                             <Hashtag hashtag={text}/>
//                         </Button>
//                     ))
//                 }
//             </Stack>
//             </Box>
//         </div>
//       );
// }

import React,{ useEffect, useState } from 'react';
import Hashtag from './Hashtag';
import axios from 'axios';
import { Stack, Box, Button, Typography } from '@mui/material';
import "./Widgets.css";

export default function TrendingHashtags(props) {
    const [hashtags,setHashtags] = useState([])
    useEffect(()=>{
        axios.get('http://localhost:8000/hashtags')
        .then(response=>{
            console.log('Received response from get hashtags endpoint')
            console.log(response)
            setHashtags(response.data.slice(0,5))
        })
    },[])

    return (
        <div className="widgets">
            <Box sx={{ width: '100%' }}>
            <Stack spacing={2}>

            <Typography variant='subtitle1' display='block'>
                <b>Trending hashtags</b>
            </Typography>
                {hashtags.map((hash)=>
                    (
                        <Button variant='text' color='inherit' style={{justifyContent: "start", textTransform: 'none'}}>
                            <Hashtag hashtag={hash.hashtag} count={hash.count}/>

                        </Button>
                    ))
                }
            </Stack>
            </Box>
        </div>
      );
}